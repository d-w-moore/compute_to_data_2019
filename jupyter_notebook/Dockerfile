FROM jupyter/base-notebook

ARG  irods_gid=999
ENV  IRODS_GID ${irods_gid}

USER root

RUN apt-get update && apt-get install -y vim less

RUN groupadd -g $IRODS_GID irods && usermod -aG irods jovyan 

#RUN /opt/conda/bin/conda init 

#RUN /opt/conda/bin/conda install -y -c conda-forge matplotlib numpy

ADD demochirp.ipynb /home/jovyan/work/.

COPY mymodule/ /home/jovyan/work/mymodule/

RUN chown jovyan.users /home/jovyan/work/demochirp.ipynb

USER jovyan

RUN conda init

RUN conda install -y -c conda-forge matplotlib numpy

RUN jupyter trust /home/jovyan/work/demochirp.ipynb

COPY mymodule/ /home/jovyan/work/mymodule

CMD [ '/bin/bash' ]
