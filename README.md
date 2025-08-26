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
Another approach, producer-consumer pipeline, which is implemented here, is based on overlapping between these 2 pipelines so that when first 
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

