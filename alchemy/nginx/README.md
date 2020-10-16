# HTTPS via Nginx

When you want to expose your server via HTTPS, use Nginx. We'll use it for SSL termination and reverse proxy.

Setup:

1. `cd` into this folder.
2. Unless you have your own domain and your own certificate, you'll need to generate your own certificate:
    ```
    openssl req -x509 -nodes -days 365 -subj "/C=CA/ST=QC/O=Company, Inc./CN=alchemy.dev" -addext "subjectAltName=DNS:alchemy.dev" -newkey rsa:2048 -keyout nginx-selfsigned.key -out nginx-selfsigned.crt
    ```
    This creates two cert files in this folder.
3. Build a custom nginx docker file with our certificates included:
    ```
    docker build -t my-nginx .
    ```
4. Go back to the parent folder, and add an `nginx` service to `docker-compose.yml`:
    ```
    services:

        nginx:
            image: my-nginx
            build:
            context: ./nginx
            dockerfile: ./nginx/Dockerfile
            ports:
            - "443:443"
            depends_on:
            - backend
            - frontend

        ...
    ```
    This service will listen on port 443, and route requests to the backend and frontend servers.
5. Let's say you don't have a domain, so your service is exposed on an IP, e.g. https://34.123.234.45, you actually can't access it directly, because the server can't tell if you're trying to access the backend or the frontend! To fix this, you can edit your local `host` file (your local DNS cache) to force made-up server names to point to that IP address. On a Mac, do the following:
    1. `sudo vi /etc/hosts` (use favourite editor)
    2. Add these lines to the bottom, substitute in your server's IP:
        ```
        34.123.234.45 backend.alchemy.dev
        34.123.234.45 frontend.alchemy.dev
        ```
    3. You may need to run `sudo dscacheutil -flushcache` for this to take effect.
    4. Visit https://backend.alchemy.dev or https://frontend.alchemy.dev in your browser.
6. If you have generated your own certificate in a previous step, Chrome will complain that this certificate is invalid. You can [type "thisisunsafe"](https://dev.to/brettimus/this-is-unsafe-and-a-bad-idea-5ej4) into Chrome to bypass the warning (note: it won't echo what you typed).
