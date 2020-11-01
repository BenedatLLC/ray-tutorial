FROM rayproject/ray:latest
RUN apt-get -y -q update
RUN apt-get -y -q install vim-tiny # for looking at files
# install the packages we will need for our demos
RUN conda install scikit-learn jupyter ipython

VOLUME /host
WORKDIR /host
CMD /bin/bash
