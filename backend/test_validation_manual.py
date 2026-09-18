from media_validation import validate_media_file, InvalidMediaError

# apna koi bhi test video file ka path yahan daalo
test_file = "fake_video.mp4"  # isko replace karna apni test file ke path se

try:
    info = validate_media_file(test_file)
    print("VALID:", info)
except InvalidMediaError as e:
    print("INVALID:", e)