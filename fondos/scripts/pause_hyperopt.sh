kill -STOP $(ps aux | grep hyper | awk '{print $2}')
