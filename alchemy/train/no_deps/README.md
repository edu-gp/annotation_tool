```
export PROJECT_ID=<Your GCP Project ID>
export IMAGE_REPO_NAME=alchemy
export IMAGE_TAG=v1
export IMAGE_HOST=us.gcr.io
export IMAGE_URI=$IMAGE_HOST/$PROJECT_ID/$IMAGE_REPO_NAME:$IMAGE_TAG

# Build it locally and push
docker build -t $IMAGE_URI .
docker push $IMAGE_URI
# OR, submit directly to cloud build
gcloud builds submit --tag $IMAGE_URI

# If you run into issues with authentication, try running the following to log in:
gcloud auth print-access-token | docker login -u oauth2accesstoken --password-stdin https://$IMAGE_HOST

# You'll receive a sha:... value, keep track of that and put it inside your .env file
```
