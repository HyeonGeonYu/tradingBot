# Trading Bot

# wsl에서 docker 빌딩(잘안됨)
docker build -t trading-bot .
cd /mnt/c/Users/Hyeongeon/Hansoldeco_s3_comp/tradingBot
docker run -p 8000:8000 trading-bot

# test
cmd에서
start chrome --remote-debugging-port=9222 --user-data-dir="C:\chrometemp"

cd C:\Users\Hyeongeon\Hansoldeco_s3_comp\tradingBot
uvicorn app.main:app --port 8001
수동order테스트 : http://localhost:8001/docs#/default/manual_sell_sell_post