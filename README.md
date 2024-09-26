# nuxeo_merritt

Code for pushing [Nuxeo](https://nuxeo.cdlib.org) content into [Merritt](https://merritt.cdlib.org/). The `nuxeo_merritt.py` script creates an ATOM feed for each collection that is set up in the [Registry](https://registry.cdlib.org/) for deposit into Merritt. These ATOM feeds are stored on S3.

The feed creation job is launched weekly by Airflow, which runs each feed creation task in parallel on ECS.

## Basic Layout

**nuxeo_merritt.py** - script that generates ATOM feeds for each collection configured in the [Registry](https://registry.cdlib.org/) for Merritt deposit. The script will only update the feed if there have been updates to the collection in Nuxeo since the last run.

**run_nuxeo_merritt_task.py** - script for running `nuxeo_merritt.py` in Fargate (ECS).

**Dockerfile** - Dockerfile used for building the image that's run in ECS (or locally, if you want)

**sceptre/** - contains CloudFormation template for creating the AWS infrastructure needed to run the code in Fargate.

**dags/** - contains the Airflow DAGs for the repo

## Continuous Deployment

When new code is pushed/merged into the main branch of this repo on github, 2 CodeBuild projects are triggered:

1. `nuxeo_merritt-application-deploy` - builds the nuxeo merritt ATOM feed docker image and push to ECR
2. `nuxeo_merritt-dags-deploy` - deploys DAGs and other code to Airflow

These CodeBuild projects are defined in the `sceptre/templates/fargate.yaml` CloudFormation template.

It's a good idea to check that both deployments have succeeded after merging a PR into main, as both should be on the same version of the code.

## Local Development

Create a python virtual environment. Currently this code is run in a python 3.11 Docker container.

Create an `env.local` file and source it. You can copy `env.local.example` and update the values as needed. (As noted in comments, some of the env vars are only necessary when running in mwaa local-runner.) Source this file.

To create the ATOM feed for collection 30:

```
python nuxeo_merritt.py --collection 30
```

You can alternatively run the feeds for all collections, but be aware that this will take a long time:

```
python nuxeo_merritt.py --all
```

## Docker Development

Create an `env.docker` file. You can copy `env.docker.example` and update the values as needed. Source this file.

Login to ECR public:

```
aws ecr-public get-login-password --region us-east-1 | docker login --username AWS --password-stdin public.ecr.aws
```

Build the docker image:

```
docker compose -f compose-dev.yaml build
```


Run the docker image:


```
docker compose -f compose-dev.yaml up
```

This will run the command defined in `compose-dev.yaml`. Currently this is `["--collection", "30"]`, so the feed for collection 30 will be generated.

## Airflow Development

### Set up `aws-mwaa-local-runner`

[Note: the following is based on the model (and documentation) we are using for [Rikolti](https://github.com/ucldc/rikolti/?tab=readme-ov-file#airflow-development). You can read somewhat more extensive notes on Airflow development in that README. We should streamline some things now that we have more than just Rikolti in Airflow.]

AWS provides the [aws-mwaa-local-runner](https://github.com/aws/aws-mwaa-local-runner) repo, which provides a command line interface (CLI) utility that replicates an Amazon Managed Workflows for Apache Airflow (MWAA) environment locally via use of a Docker container. We have forked this repository and made some small changes to enable us to use local-runner while keeping our dags stored in this and other repositories.

To set up the airflow dev environment, clone the repo locally:

```
git clone git@github.com:ucldc/aws-mwaa-local-runner.git
```

Then, copy aws-mwaa-local-runner/docker/.env.example:

```
cp aws-mwaa-local-runner/docker/.env.example aws-mwaa-local-runner/docker/.env
```

If you have not already, you should also clone the `nuxeo_merritt` repo locally.

> Note: The location of the nuxeo_merritt repo relative to the aws-mwaa-local-runner repo does not matter - we will configure some environment variables so the aws-mwaa-local-runner can find nuxeo_merritt.

```
git clone git@github.com:ucldc/nuxeo_merritt.git
```

Back in `aws-mwaa-local-runner/docker/.env`, set the following env vars to wherever you have cloned the nuxeo_merritt repository, for example:

```
DAGS_HOME="/Users/bhui/dev/nuxeo_merritt"
PLUGINS_HOME="/Users/bhui/dev/nuxeo_merritt/plugins"
REQS_HOME="/Users/bhui/dev/nuxeo_merritt/dags"
STARTUP_HOME="/Users/bhui/dev/nuxeo_merritt/dags"
DATA_HOME="/Users/bhui/dev/nuxeo_merritt/output"
DOCKER_SOCKET="/var/run/docker.sock"
```

Then, modify the `volumes` section of `aws-mwaa-local-runner/docker/docker-compose-local.yml` to look like the following:

```
        volumes:
            - "${DAGS_HOME}:/usr/local/airflow/dags/nuxeo_merritt"
            - "${PLUGINS_HOME}:/usr/local/airflow/plugins"
            - "${REQS_HOME}:/usr/local/airflow/requirements"
            - "${STARTUP_HOME}:/usr/local/airflow/startup"
            - "${DATA_HOME}:/usr/local/airflow/nuxeo_merritt_data"
            - "${DOCKER_SOCKET}:/var/run/docker.sock"

```

Next, back in the nuxeo_merritt repository, create the `startup.sh` file by running `cp env.local.example dags/startup.sh`. Update the startup.sh file with the appropriate values.

The folder located at `OUTPUT_MOUNT` is mounted to `/output` on the nuxeo_merritt docker container.

You can specify a `NUXEO_MERRITT_IMAGE` and `NUXEO_MERRITT_VERSION` through environment variables as well. The default value for `NUXEO_MERRITT_IMAGE ` is `public.ecr.aws/b6c7x7s4/nuxeo/nuxeo_merritt` and the default value for `NUXEO_MERRITT_VERSION` is `latest`.

If you would like to run the nuxeo_merritt code on AWS infrastructure using the ECS operator, or if you are deploying the code to MWAA, you can specify `CONTAINER_EXECUTION_ENVIRONMENT='ecs'` (you'll need some AWS credentials as well). The `CONTAINER_EXECUTION_ENVIRONMENT` is, by default, a docker execution environment.

If you would like to run your own nuxeo_merritt image instead of pulling the image from AWS, then from inside the nuxeo_merritt repo, run `docker build -t nuxeo_merritt .` to build the `nuxeo_merritt` image locally and add the following line to `dags/startup.sh` to update `NUXEO_MERRITT_IMAGE ` to be `nuxeo_merritt`:

```
export NUXEO_MERRITT_IMAGE = nuxeo_merritt
```

If you would like to mount your own codebase to the nuxeo_merritt container run via a DockerOperator in Airflow, then add the following to `dags/startup.sh`:

```
export MOUNT_CODEBASE=<path to nuxeo_merritt, for example: /Users/bhui/dev/nuxeo_merritt>
```

Also make sure to set your temporary AWS credentials:

```
export AWS_ACCESS_KEY_ID=
export AWS_SECRET_ACCESS_KEY=
export AWS_SESSION_TOKEN=
```

Finally, from inside the aws-mwaa-local-runner repo, run `./mwaa-local-env build-image` to build the docker image, and `./mwaa-local-env start` to start the mwaa local environment.

For more information on `mwaa-local-env`, look for instructions in the [ucldc/aws-mwaa-local-runner README](https://github.com/ucldc/aws-mwaa-local-runner/#readme) to build the docker image, run the container, and do local development.

## Infrastructure-as-Code Development

The AWS infrastructure needed to run the nuxeo_merritt code in Fargate is created via a CloudFormation template. This template can be found in the `sceptre/` directory. See the [sceptre documentation](https://docs.sceptre-project.org/latest/) for information on how to install and use sceptre.

To create/update the infrastructure:

```
source env.local
cd sceptre
sceptre launch -y fargate.yaml
```

This creates 2 CodeBuild projects and an ECS task, as described at the top of this page.

If you want to kick off the ECS task from your local machine, use the `run_nuxeo_merritt_task.py` script, e.g.:

```
python run_nuxeo_merritt_task.py --collection 30
```

## Interpreting the results of deposits from Nuxeo to Merritt

Each object deposited in Merritt contains the following key files:


<b>1) mrt-erc.txt</b><br>
Merritt metadata record for the object.


<b>2) XML files</b><br>
Nuxeo metadata record. For simple objects, there will be a single metadata record. For complex objects, there will be a metadata record for the parent-level component; there will also be metadata records for each child-level component, if applicable.


<b>3) Content files (TIFF, etc.)</b><br>
Main content file and any auxiliary files, imported into Nuxeo.  For complex objects, there will be content files for the parent-level and child-level components.


<b>4) JSON file (media.json)</b><br>
JSON file reflects the structure of the object. For complex objects, the JSON will indicate the association between the  parent-level component metadata record and main content file; and likewise, the association between child-level component metadata record and main content file.


The JSON file can be used to interpret the complex object structure. In the JSON for the example object listed below, the `"id":` indicates the XML file (Nuxeo metadata record) and the `"href":` indicates the main content file (a TIFF).

[json example simplified]
```json
{
  "label": "Here comes the band = Ya viene la banda",
  "href": "https://nuxeo.cdlib.org/nuxeo/nxbigfile/default/5bdd2118-e3e1-4a5c-b2e9-7675fbdc8106/file:content/gen_n7433_4m648h47_001.TIF",
  "id": "5bdd2118-e3e1-4a5c-b2e9-7675fbdc8106",
  "structMap": [
    { 
      "href": "https://nuxeo.cdlib.org/nuxeo/nxbigfile/default/ddd02f15-5fee-44a1-9f99-1bcda6e36436/file:content/gen_n7433_4m648h47_002.TIF",
      "label": "General view",
      "id": "ddd02f15-5fee-44a1-9f99-1bcda6e36436"
    },
    {
      "href": "https://nuxeo.cdlib.org/nuxeo/nxbigfile/default/dda4ca90-c5b7-4ec4-9fcd-3b2e394ef050/file:content/gen_n7433_4m648h47_003.TIF",
      "label": "Detail view",
      "id": "dda4ca90-c5b7-4ec4-9fcd-3b2e394ef050"
    }
  ]
}
```

The individual XML files comprise the complete metadata records.  Note that the XML files also reflect their associated content files, in the `<schema name="file">` and `<schema name="extra_files">` entries.  In the example object, `<schema name="file">` points to the main content file (a TIFF) and `<schema name="extra_files">` points to the auxiliary files (DNG files).   


(Note that the `<picture:views>` entries for JPEG files can be ignored: the `<picture:views>` reference derivative images generated within Nuxeo for display in that context; those files are not deposited into Merritt).



## Example object deposited from Nuxeo to Merritt

<b>Morales, Gloria. Here comes the band = Ya viene la banda (c1999)</b><br>
<b>object primary identifier:</b>  ark:/99999/fk4mk6km9t<br>
<b>permanent link:</b>  http://merritt.cdlib.org/m/ark%3A%2F99999%2Ffk4mk6km8d/1<br>
<b>title:</b>  Here comes the band = Ya viene la banda<br>
<b>creator:</b>  Morales, Gloria<br>
<b>date:</b>  c1999<br>
<b>local id:</b>  5bdd2118-e3e1-4a5c-b2e9-7675fbdc8106<br>
<b>version number:</b>  1<br>
<b>version date:</b>  2016-10-07 02:15 PM UTC<br>
<b>version size:</b>  281.9 MB<br>
<b>version files:</b>  19<br>

### User Files
<b>mrt-erc.txt</b>  text/plain 155 B<br> 
<b>5bdd2118-e3e1-4a5c-b2e9-7675fbdc8106.xml</b>   application/xml 17.9 KB<br>
<b>dda4ca90-c5b7-4ec4-9fcd-3b2e394ef050.xml</b>   application/xml 12 KB<br> 
<b>ddd02f15-5fee-44a1-9f99-1bcda6e36436.xml</b>   application/xml 12 KB<br> 
<b>gen_n7433_4m648h47_001.dng</b>   image/tiff 50.7 MB<br> 
<b>gen_n7433_4m648h47_001.TIF</b>   image/tiff 44 MB<br> 
<b>gen_n7433_4m648h47_003.dng</b>   image/tiff 40.8 MB<br>
<b>gen_n7433_4m648h47_003.TIF</b>   image/tiff 37 MB<br> 
<b>gen_n7433_4m648h47_002.dng</b>   image/tiff 49.4 MB<br> 
<b>gen_n7433_4m648h47_002.TIF</b>   image/tiff 59.9 MB<br> 
<b>5bdd2118-e3e1-4a5c-b2e9-7675fbdc8106-media.json</b>   application/json 1.6 KB<br> 

### System Files
<b>mrt-dc.xml</b>   application/xml 149 B<br> 
<b>mrt-erc.txt</b>   text/plain 158 B<br> 
<b>mrt-ingest.txt</b>   text/plain 1.6 KB<br> 
<b>mrt-membership.txt</b>   text/plain 20 B<br> 
<b>mrt-mom.txt</b>   text/plain 136 B<br> 
<b>mrt-object-map.ttl</b>  plain/turtle 5 KB<br> 
<b>mrt-owner.txt</b>   text/plain 19 B<br> 
<b>mrt-submission-manifest.txt</b>  text/plain 3.1 KB<br> 




