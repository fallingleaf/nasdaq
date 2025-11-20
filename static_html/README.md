# Static HTML Files

This directory contains static HTML files that are served directly by nginx for better performance.

## Files

- `index.html` - Landing page (home page) for Masonias.com
- `404.html` - Custom 404 error page

## Deployment

When deploying to production:

1. Copy this directory to your server:
   ```bash
   scp -r static_html/ user@server:/home/YOUR_USERNAME/nasdaq/
   ```

2. Update the `root` path in `/etc/nginx/sites-available/nasdaq` to point to this directory:
   ```nginx
   root /home/YOUR_USERNAME/nasdaq/static_html;
   ```

3. Ensure nginx has read permissions:
   ```bash
   chmod 755 /home/YOUR_USERNAME/nasdaq/static_html
   chmod 644 /home/YOUR_USERNAME/nasdaq/static_html/*.html
   ```

4. Test and reload nginx:
   ```bash
   sudo nginx -t
   sudo systemctl reload nginx
   ```

## Local Development

For local development without nginx, the Flask app will continue to serve these pages from the `src/templates/` directory using the routes in `webapp.py`.

## Benefits of Static Serving

- **Better Performance**: nginx serves static files much faster than Python
- **Lower Resource Usage**: No need to invoke Flask for simple pages
- **Higher Concurrency**: nginx can handle many more static file requests
- **Simpler Caching**: Static files are easier to cache at the edge
