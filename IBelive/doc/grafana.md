# 容器启动方法
docker run -d --name=grafana -p 3000:3000 grafana/grafana

docker run -d --name=grafana -p 3000:3000 -v /Users/gelin/Desktop/store/dev/python/20250926stock/grafana/data/:/var/lib/grafana grafana/grafana

docker run -d --name=grafana -p 3000:3000 -v /Users/gelin/Desktop/store/dev/python/20250926stock/grafana/config/:/etc/grafana/ -v /Users/gelin/Desktop/store/dev/python/20250926stock/grafana/data/:/var/lib/grafana grafana/grafana


# 访问方法
http://localhost:3000/
默认密码：admin/admin