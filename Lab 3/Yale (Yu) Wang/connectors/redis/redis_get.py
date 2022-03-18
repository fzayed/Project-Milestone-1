import redis        # pip install redis
ip='34.130.3.118';
r = redis.Redis(host=ip, port=6379, db=1,password='SOFE4630U')
v=r.get('1357847242531979.tiff_pic21');
with open("./recieved.jpg", "wb") as f:
    f.write(v);
