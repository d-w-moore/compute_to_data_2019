FROM jupyter/base-notebook

USER root

RUN apt-get update && apt-get install -y vim 

#RUN sudo su - jovyan -c "conda install -y -c conda-forge matplotlib"

RUN /opt/conda/bin/conda init 

RUN /opt/conda/bin/conda install -y -c conda-forge matplotlib

USER jovyan

CMD [ '/bin/bash' ]
