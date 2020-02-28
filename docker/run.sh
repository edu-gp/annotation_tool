echo "Starting Server"

cd /annotation_tool
supervisord -n -c docker/supervisord.conf