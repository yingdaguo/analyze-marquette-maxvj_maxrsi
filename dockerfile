FROM python:3.7-slim-buster

# copy apps
COPY app/ app/

# make working directory
WORKDIR app/

# install requirements
RUN pip install -r requirements.txt

# Create a user app
RUN groupadd -r app &&\
    useradd -r -g app -d /home/app -s /sbin/nologin -c "Docker image user" app


# Chown all the files to the app user.
RUN chown -R app:app /app

USER app

ENTRYPOINT [ "python", "-u", "app.py" ]