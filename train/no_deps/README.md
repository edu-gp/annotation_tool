```
export PROJECT_ID=<Your GCP Project ID>
export IMAGE_REPO_NAME=alchemy
export IMAGE_TAG=v1
export IMAGE_URI=us.gcr.io/$PROJECT_ID/$IMAGE_REPO_NAME:$IMAGE_TAG

# Build it locally and push
docker build -t $IMAGE_URI .
docker push $IMAGE_URI
# OR, submit directly to cloud build
gcloud builds submit --tag $IMAGE_URI

# You'll receive a sha:... value, keep track of that and put it inside your .env file
```
