kill -9 $(ps aux | grep hyper | awk '{print $2}')
