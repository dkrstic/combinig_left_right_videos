import subprocess
import threading
import os
import time
import glob
from concurrent.futures import ThreadPoolExecutor, wait, ALL_COMPLETED
from itertools import product

"""
Since it's a Cartesian product between 2 input video sets, there are many readings/decodings per video and the goal is to
reduce the decoding costs by creating intermediate files with low decoding footprint. However, this increases the storage 
usage. The assumption here is that processing power and video pipeline performance is of higher priority.
There 2 video processing pipelines:
- One is for massive video transformation (transform_video) of input videos, left and right, to corresponding intermediate 
left/right folders that contains intermediate left/right videos with video codecs that are much less expensive for 
decoding than original.
This pipeline requires 'expensive' decoding of original video but it happens only once per video.
- The second video processing pipeline (join_videos) combines the videos between 2 intermediate video sets and create 
resulting video combinations.
The strait-forward approach would be first to finish all transformation, then to start final processing with creation of
all output combination.
Another approach, which is implemented here is based on overlapping between these 2 pipelines so that when first 
transformation finished and first few intermediate files from both sets become available their further processing and 
video joining can start.
What is more performant and/or beneficial could be a subject of particular technical goals and can be measured by manual
testing, depending of available hw and others.
For example, if we want to generate video combinations as soon as possible for fast publishing or similar then 
overlapping approach could be more beneficial. Another example is when the first video processing pipeline runs on
the first host (video transform producer), intermediate videos are written to the share folder, where video combiner 
(consumer on another host) reads them from, combines them and write to the final destination.
In this improvised producer-consumer pipeline, consumer (video-combiner-encoder) doesn't have to wait for all videos to 
be transformed first before start consume/combine them.
But if we still want for some reason to start the second (combiner) pipeline only after the first pipeline (decoder)
completely finishes this can be simply done by adjusting the indentations for ThreadPoolExecutor in this solution. 
"""

# Max number of concurrent ffmpeg processes
MAX_CONCURRENT_DEC_PROCESSES = 30
MAX_CONCURRENT_ENC_PROCESSES = 30
MAX_ENC_TIME = 10800  # e.g. 3h

# ad-hoc trivial approach to control processor utilization of 2 different video processing pipelines
# for example, tuned values obtained after few manual tries to keep cpu utilization <70%
INTER_DEC_TIME = 0.5  # decoding thread pause time after finishing its decoding and writing to intermediate files
INTER_ENC_TIME = 0.5  # encoding thread pause time after finishing its encoding and writing to resulting video combination

left_videos = glob.glob("left_dir/video*.mp4")
right_videos = glob.glob("right_dir/video*.mp4")

transformed_left = set()
transformed_right = set()
all_video_combinations = set()

# Directory for output files
output_left_dir = "transformed_left_videos"
output_right_dir = "transformed_right_videos"
final_output_dir = "video_output"
os.makedirs(output_left_dir, exist_ok=True)
os.makedirs(output_right_dir, exist_ok=True)

lock_transformed_left = threading.Lock()
lock_transformed_right = threading.Lock()


def transform_video(input_file, lock, side="left"):
    global transformed_left
    global transformed_right
    crop = 'crop=iw/2:ih:0:0' if side == 'left' else 'crop=iw/2:ih:iw/2:0'
    output_dir = output_left_dir if side == 'left' else output_right_dir
    output_file = os.path.join(output_dir, f"{os.path.basename(input_file)}")
    try:
        # convert original video to an intra frame decoding video so that expensive decoding happens only once per
        # each input video
        cmd = [
            'ffmpeg',
            '-i', input_file,
            '-vf', crop,  # Crop left or right
            '-c:v', 'png', '-pix_fmt', 'rgb24', '-y',
            output_file,
        ]
        print(f"Starting video transformation {input_file}")
        result = subprocess.run(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        time.sleep(INTER_DEC_TIME)
        if result.returncode != 0:
            print(f"Error video transformation {input_file}: {result.stderr.decode()}")
        else:
            print(f"Finished video transformation {input_file}")
            with lock:
                if side == "left":
                    transformed_left.add(output_file)
                else:
                    transformed_right.add(output_file)
    except Exception as e:
        print(f"Exception video transformation {input_file}: {e}")


def join_videos(input_file_left, input_file_right):
    output_basename = f"{os.path.basename(input_file_left)[:-4]}" + f"_{os.path.basename(input_file_right)}"
    output_file = os.path.join(final_output_dir, output_basename)
    try:
        # concat frames from 2 videos into one frame in resulting video
        cmd = [
            'ffmpeg',
            '-i', input_file_left,
            '-i', input_file_right,
            '-filter_complex', 'hstack=inputs=2', '-y',
            output_file,
        ]
        print(f"Writing to {output_file}")
        result = subprocess.run(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        time.sleep(INTER_ENC_TIME)
        if result.returncode != 0:
            print(f"Error writing to {output_file}: {result.stderr.decode()}")
        else:
            print(f"Finished writing to {output_file}")
    except Exception as e:
        print(f"Exception writing to {output_file}: {e}")


"""
Using ThreadPoolExecutor to schedule video transformation tasks
Transform left and right videos to intermediate video files, once per each video. 
Intermediate video files require much less expensive decoding for repeating reading/decoding to create all possible
video combinations
"""
# Transform left
with ThreadPoolExecutor(max_workers=int(MAX_CONCURRENT_DEC_PROCESSES / 2)) as executor:
    futures_left = [executor.submit(transform_video, vf, lock_transformed_left) for vf in left_videos]

    # Transform right
    with ThreadPoolExecutor(max_workers=int(MAX_CONCURRENT_DEC_PROCESSES / 2)) as executor:
        futures_right = [executor.submit(transform_video, vf, lock_transformed_right, "right") for vf in right_videos]

        start_enc_time = time.time()
        # merge left and right videos and make all video combinations
        total_outputs_number = len(left_videos) * len(right_videos)
        while len(all_video_combinations) < total_outputs_number or time.time() - start_enc_time > MAX_ENC_TIME:
            # lock transformed_left and transformed_right to create all available combination at a given moment if we want to
            # start creating of final video output combinations from intermediate files before all these files are created by
            # overlapping video transformation and video combination tasks. However, the benefit of this approach is questionable,
            # for example, on a single host, comparing with complete-transformation-then-complete-processing approach
            with lock_transformed_left:
                with lock_transformed_right:
                    prepared_combinations = set(product(transformed_left, transformed_right))
                    print(prepared_combinations)
            with ThreadPoolExecutor(max_workers=MAX_CONCURRENT_ENC_PROCESSES) as executor:
                futures = [executor.submit(join_videos, vt[0], vt[1]) for vt in prepared_combinations]
                done, not_done = wait(futures, return_when=ALL_COMPLETED)
                all_video_combinations = all_video_combinations | prepared_combinations
                prepared_combinations.clear()
