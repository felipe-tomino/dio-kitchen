FROM node:12

RUN mkdir -p /home/node/app/node_modules && chown -R node:node /home/node/app

WORKDIR /home/node/app

COPY package*.json ./

USER node

RUN yarn

COPY --chown=node:node . .

ARG CONSUMER_TYPE
ENV CONSUMER_TYPE $CONSUMER_TYPE

# EXPOSE 8080
# CMD [ "yarn", "prod" ]
