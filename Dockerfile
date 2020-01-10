FROM my-highload-cup:latest
ADD file:c0f17c7189fc11b6a1d525e9fcff842b93bfaae92e5b91cb9a76aa867792756d in /
RUN /bin/sh -c set -xe 		\
    && echo '#!/bin/sh' > /usr/sbin/policy-rc.d 	\
    && echo 'exit 101' >> /usr/sbin/policy-rc.d 	\
    && chmod +x /usr/sbin/policy-rc.d 		\
    && dpkg-divert --local --rename --add /sbin/initctl 	\
    && cp -a /usr/sbin/policy-rc.d /sbin/initctl 	\
    && sed -i 's/^exit.*/exit 0/' /sbin/initctl 		\
    && echo 'force-unsafe-io' > /etc/dpkg/dpkg.cfg.d/docker-apt-speedup 		\
    && echo 'DPkg::Post-Invoke { "rm -f /var/cache/apt/archives/*.deb /var/cache/apt/archives/partial/*.deb /var/cache/apt/*.bin || true"; };' > /etc/apt/apt.conf.d/docker-clean 	\
    && echo 'APT::Update::Post-Invoke { "rm -f /var/cache/apt/archives/*.deb /var/cache/apt/archives/partial/*.deb /var/cache/apt/*.bin || true"; };' >> /etc/apt/apt.conf.d/docker-clean 	\
    && echo 'Dir::Cache::pkgcache ""; Dir::Cache::srcpkgcache "";' >> /etc/apt/apt.conf.d/docker-clean 		\
    && echo 'Acquire::Languages "none";' > /etc/apt/apt.conf.d/docker-no-languages 		\
    && echo 'Acquire::GzipIndexes "true"; Acquire::CompressionTypes::Order:: "gz";' > /etc/apt/apt.conf.d/docker-gzip-indexes 		\
    && echo 'Apt::AutoRemove::SuggestsImportant "false";' > /etc/apt/apt.conf.d/docker-autoremove-suggests
RUN /bin/sh -c rm -rf /var/lib/apt/lists/*
RUN /bin/sh -c mkdir -p /run/systemd \
    && echo 'docker' > /run/systemd/container
CMD ["/bin/bash"]
ENV HANDLER=echo build failed. You can debug this container by running \`docker exec -it THIS_CONTAINER_ID sh\`. \(available for 1 hour\); sleep 3600
RUN /bin/sh -c apt-get update \
    && apt-get install -y build-essential unzip autoconf
COPY dir:c054d7401eee001ca883bcda3afc58e39ba526ef99ee9f3989a3bf2d1610a09b in /third_party
WORKDIR /third_party/civetweb
RUN /bin/sh -c make clean
COPY dir:9ddf00fc957b86b0518f00ed4f6fd3011761a4bf0fa152a9a0320b2bff873713 in /src
WORKDIR /src/my_webserver
RUN /bin/sh -c make clean
CMD ["/bin/sh" "-c" "./start.sh"]
