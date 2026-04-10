FROM nodered/node-red:4.1.8

USER root

RUN cd /usr/src/node-red \
    && npm install --unsafe-perm --no-update-notifier --no-fund \
        node-red-dashboard \
        node-red-contrib-mongodb3 \
        proj4

USER node-red
