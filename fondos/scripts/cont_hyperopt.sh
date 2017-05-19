kill -CONT $(ps aux | grep hyper | awk '{print $2}')
