FROM ubuntu:20.04

ENV DEBIAN_FRONTEND=noninteractive

RUN apt-get update \
  && apt-get install -y git python3-pip python3-dev libmagic-dev libvips ffmpeg \
  && cd /usr/local/bin \
  && ln -s /usr/bin/python3 python \
  && pip3 --no-cache-dir install --upgrade pip \
  && rm -rf /var/lib/apt/lists/*

ENV APP_HOME /usr/src/app

WORKDIR $APP_HOME

RUN python3 -m pip install awslambdaric

COPY requirements.txt ./
RUN pip install -r requirements.txt

COPY .  ./

ENTRYPOINT [ "/usr/local/bin/python", "-m", "awslambdaric" ]
CMD ["main.handler"]