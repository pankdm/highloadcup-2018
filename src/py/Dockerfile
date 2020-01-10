# Inheriting from CentOS 7
FROM centos:7

# Chosing a working directory
WORKDIR /src

# # Installing wget and downloading Go
# RUN yum install -y wget && \
#     wget https://storage.googleapis.com/golang/go1.8.3.linux-amd64.tar.gz
#
# # Installing Go, creating workspace and project directory
# RUN tar -C /usr/local -xzf go1.8.3.linux-amd64.tar.gz && \
#     mkdir go && mkdir go/src && mkdir go/bin && mkdir go/pkg && \
#     mkdir go/src/dumb
#
# # Setting environment variables for Go
# ENV PATH=${PATH}:/usr/local/go/bin GOROOT=/usr/local/go GOPATH=/root/go
#
# # Copying our source file main.go inside the container to go/src/dumb folder
# ADD main.go go/src/dumb
#
# # Compiling and installing our server
# RUN go build dumb && go install dumb

ADD webapp.py /src/

# Exposing port 80
EXPOSE 80

# Launching our server
CMD ["python", "./webapp.py"]
# CMD ["python", "-m", "SimpleHTTPServer", "80"]
