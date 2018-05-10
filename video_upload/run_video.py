#!/usr/bin/env python

import progressbar
import os.path
import argparse
import pymongo
from pymongo import MongoClient
import datetime, time
from bson import Binary
import cv2 
import cPickle

DATABASE_NAME = "myImageDatabase"
COLLECTION_NAME = "myImageCollection"
    
# dimension of snapshot samples to show
SNAPSHOT_DISPLAY_WIDTH = 300  
SNAPSHOT_DISPLAY_HEIGHT = 200

IMAGE_DIR = '/Users/wallacewong/mongodb/images/' # image directory to produce output

X_MOVEMENT = 310  # horizontal displacement for snapshot
Y_MOVEMENT = 235  # vertical displacement for snapshot
NUMBER_OF_SNAPSHOTS = 9 
NUM_MOVE_INTERVALS = 10    # number of intervals to move the snapshot before the desired location
SNAPSHOT_INTERVAL = 100    # frame interval to take a snapshot
NUM_SNAPSHOTS_IN_A_ROW = 3 # number of snapshots to show in a row

   
# show a frame from a location and move to destination location
def move_window(frame_name, image, start_x, start_y, end_x, end_y, number_of_moves, show_image=True):  
    x_step = (end_x - start_x) / number_of_moves
    y_step = (end_y - start_y) / number_of_moves
    if show_image:
        resized_image = cv2.resize(image, (SNAPSHOT_DISPLAY_WIDTH, SNAPSHOT_DISPLAY_HEIGHT))               
        cv2.imshow(frame_name, resized_image)

    for i in range(number_of_moves):
        cv2.namedWindow(frame_name,cv2.WINDOW_NORMAL)
        cv2.moveWindow(frame_name, start_x + i*x_step, start_y + i*y_step)   
        cv2.waitKey(1)
        #if(not show_image):
        #    time.sleep(0.05)
            
    if(not show_image):
        cv2.moveWindow(frame_name, end_x, end_y)   


# extract frames from a video file and ingest into local mongodb
def extract_frames(video_file, video_timestamp_str):
    client = MongoClient()

    # specify the collection for storing frames
    db = client[DATABASE_NAME]
    collection = db[COLLECTION_NAME]
    collection.remove()
    collection.ensure_index([( 'created_at', 1)])
    
    vidcap = cv2.VideoCapture(video_file)
    video_full_filename, video_file_ext = video_file.split(".")    
    video_filename = os.path.basename(video_full_filename)
    video_timestamp = datetime.datetime.strptime(video_timestamp_str, "%Y-%m-%d %H:%M:%S")
    #print("video_timestamp: %s" % video_timestamp)
   
    fps = vidcap.get(cv2.CAP_PROP_FPS)
    ms_per_frame = int(1000000 / fps)
    print('\n')
    print("################################################################################")
    print("#############     VIDEO INFORMATION      #######################################")
    print("################################################################################")
    print("# {:25}| {:<50}#" .format ("Input video:", video_file))
    print("# {:25}| {:<50}#" .format ("Frame per second:", fps))
    print("# {:25}| {:<50}#" .format ("Microsecond per frame:", ms_per_frame))
    num_of_frames = vidcap.get(cv2.CAP_PROP_FRAME_COUNT)
    print("# {:25}| {:<50}#" .format ("Total number of frames:", int(num_of_frames)))
    print("# {:25}| {:<50}#" .format ("Video starts from:", video_timestamp_str))
    print("################################################################################")
    print('\n')
    print("Extracting frames from video ......")
    bar = progressbar.ProgressBar(maxval=num_of_frames, widgets=[progressbar.Bar('=', '[', ']'), ' ', progressbar.Percentage()])
    bar.start()
    cnt = 0
    show_cnt = 0
    image_samples = {}
    success,image = vidcap.read()
    posts = []
    start_datetime = datetime.datetime.now()
    while success:
        frame_name = video_filename + "_" + str(cnt).zfill(5) + ".png"
        
        # save the snapshots to display
        if cnt % SNAPSHOT_INTERVAL == 0 and show_cnt < NUMBER_OF_SNAPSHOTS:  
            image_samples[frame_name]=image
            show_cnt = show_cnt + 1

        # insert the image into mongodb
        frame_datetime = video_timestamp + datetime.timedelta(microseconds=ms_per_frame*cnt)
        image_data = Binary(cPickle.dumps(image, protocol=2))

        # ingest the frame to mongodb
        post = { 'sensor_id' : 1, 'video_name' : video_filename + '.' + video_file_ext, 'created_at' : frame_datetime , 'image' : image_data}
        posts.append(post)
        if(len(posts)%5==0):
            collection.insert_many(posts)
            posts = []
    
        # read the next frame
        success,image = vidcap.read()
        cnt = cnt + 1 
        bar.update(cnt)    
        
    if(len(posts)<>0):
        collection.insert_many(posts)
        posts = []  
    bar.finish()
    
    end_datetime = datetime.datetime.now()    
    time_taken = (end_datetime - start_datetime).total_seconds()
    print("\n=> %s image frames uploaded in %.2f seconds" % (int(num_of_frames),time_taken))
    print("=> %.2f image frames uploaded per seconds" % (num_of_frames/time_taken))
    
    print("\nDisplaying snapshots ...")
    # display the saved snapshots
    show_cnt = 0    
    winodow_positions = {}
    for frame_name in sorted(image_samples):
        x_pos = 50+(show_cnt%NUM_SNAPSHOTS_IN_A_ROW)*X_MOVEMENT
        y_pos = 50+((show_cnt)/NUM_SNAPSHOTS_IN_A_ROW)*Y_MOVEMENT
        #print('position: (%s,%s)' % (x_pos, y_pos))     
        move_window(frame_name, image_samples[frame_name], 10, 10, x_pos, y_pos, NUM_MOVE_INTERVALS)
        winodow_positions[frame_name] = (x_pos, y_pos)
        show_cnt = show_cnt + 1
        time.sleep(0.5)
    
    cv2.waitKey(0)
    print("Done\n")
    
    for frame_name in reversed(sorted(image_samples)):
        x_pos, y_pos = winodow_positions[frame_name]
        move_window(frame_name, image_samples[frame_name], x_pos, y_pos, 10,10, NUM_MOVE_INTERVALS, False)

    cv2.destroyAllWindows()
  
        
OUTPUT_VIDEO = "/Users/wallacewong/mongodb/videos/test_out.mp4"        
        
def retrieve_video(start_time_str,end_time_str,sensor_id=1): 
      
    video_start_time = datetime.datetime.strptime(start_time_str, "%Y-%m-%d %H:%M:%S")
    video_end_time = datetime.datetime.strptime(end_time_str, "%Y-%m-%d %H:%M:%S")

    client = MongoClient()
    db = client[DATABASE_NAME]
    collection = db[COLLECTION_NAME]
    
    print("\nRetrieving frame from %s to %s" % (video_start_time,video_end_time))

    all_records = collection.find( { "sensor_id" : sensor_id, 'created_at' : { "$gte" : video_start_time, "$lte" : video_end_time }}).sort( [( "created_at", pymongo.ASCENDING) ] ) #.limit(10)
        
    out = None
    cnt = 0
    for record in all_records:
        
        image_data = record["image"]
        image = cPickle.loads(image_data)
        
        if cnt==0:
            height , width , layers =  image.shape
            fourcc = cv2.VideoWriter_fourcc(*'mp4v') # Be sure to use lower case
            out = cv2.VideoWriter(OUTPUT_VIDEO, fourcc, 30.0, (width, height))
    
        out.write(image) # Write out frame to video
        cnt = cnt + 1
  
    out.release()
    print("Number of frames retrieved: [ %s ]" % cnt)

    cap = cv2.VideoCapture(OUTPUT_VIDEO)
    while(cap.isOpened()):
        ret, frame = cap.read()
        if ret == True:
            cv2.imshow('video retrieved', frame)
            # & 0xFF is required for a 64-bit system
            if cv2.waitKey(1) & 0xFF == ord('q'):
                break
        else:
            break
    cap.release()
    cv2.waitKey(0)
    cv2.destroyAllWindows()
    print('\nDone\n')


if __name__ == '__main__':

    #print("video_upload.test")
    #test_db()
    
    parser = argparse.ArgumentParser()
    parser.add_argument("--op", default="WRITE", help="WRITE or READ, default is READ")
    parser.add_argument("--input", help="Video input to extract frames")
    parser.add_argument("--timestamp", default='2018-05-01 12:00:00', help="Starting time of video")
    parser.add_argument("--start-time", default='2018-05-01 12:00:30', help="Starting time of video")
    parser.add_argument("--end-time", default='2018-05-01 12:00:40', help="Starting time of video")
    args = parser.parse_args()
    
    
    stage = args.op
    
    if(stage=='WRITE'):
        video_input = args.input
        if(video_input == None):
            print("Please provide a mp4 file for --input parameter")
        elif not os.path.exists(video_input):
            print("The video %s does not exist" % video_input)
            exit()
        extract_frames(video_input, args.timestamp)
    
    if(stage=='READ'):
        
        retrieve_video(args.start_time, args.end_time)

    
    
    
    
    