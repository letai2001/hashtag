#!/bin/bash
# start.sh

# Chạy script Python của bạn trong nền
# python run.py &

# Khởi động server FastAPI
# uvicorn main1:app --host 0.0.0.0 --port 5601 --reload
#!/bin/bash
# start.sh

# Chạy script Python của bạn trong nền và redirect log
python run_2.py > /usr/app/src/run.log 2>&1 &

# Chạy server FastAPI
uvicorn main2:app --host 0.0.0.0 --port 5601 --reload
