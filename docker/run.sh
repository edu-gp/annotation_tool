echo "Starting Server"

cd /annotation_tool
# We probably need to add some code to run database migration here before
# starting the actual server.
supervisord -n -c docker/supervisord.conf

