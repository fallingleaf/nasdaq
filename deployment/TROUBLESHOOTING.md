# Deployment Troubleshooting Guide

This guide helps diagnose and fix common deployment issues for the Masonias web application.

## Issue: /db and /services return 404 errors

### Root Causes
1. Flask app (gunicorn) not running
2. Nginx configuration not loaded
3. Nginx not proxying correctly
4. Wrong nginx config file active

---

## Step 1: Check if Gunicorn is Running

```bash
# Check if the service is running
sudo systemctl status nasdaq-webapp

# If not running, start it
sudo systemctl start nasdaq-webapp

# Check the logs
sudo journalctl -u nasdaq-webapp -n 50 --no-pager

# Check if gunicorn is listening on port 8000
sudo netstat -tlnp | grep 8000
# OR
sudo ss -tlnp | grep 8000
```

**Expected output:**
- Service should show "active (running)"
- Port 8000 should show gunicorn listening

---

## Step 2: Test Flask App Directly

```bash
# Test if Flask is responding on localhost:8000
curl http://127.0.0.1:8000/

# Test /db endpoint
curl http://127.0.0.1:8000/db/

# Test /services endpoint
curl http://127.0.0.1:8000/services/
```

**Expected output:**
- Should return HTML content
- If you get "Connection refused", gunicorn is not running

---

## Step 3: Check Nginx Configuration

```bash
# Test nginx configuration syntax
sudo nginx -t

# Check which config file is active
sudo nginx -T | grep "configuration file"

# Check if your site is enabled
ls -la /etc/nginx/sites-enabled/

# Verify the symlink points to correct file
readlink -f /etc/nginx/sites-enabled/nasdaq
```

**Expected output:**
- `nginx -t` should show "syntax is ok" and "test is successful"
- Your nasdaq config should be linked in sites-enabled

---

## Step 4: Verify Nginx Configuration Content

```bash
# View the active nginx config
sudo cat /etc/nginx/sites-enabled/nasdaq

# Or view the source config
cat /etc/nginx/sites-available/nasdaq
```

**Check for:**
1. `upstream nasdaq_webapp` pointing to `127.0.0.1:8000`
2. `location ~ ^/db(/.*)?$` block exists
3. `location ~ ^/services(/.*)?$` block exists
4. No conflicting `root` directive interfering with proxy_pass

---

## Step 5: Reload/Restart Nginx

```bash
# Test config first
sudo nginx -t

# If OK, reload nginx (zero downtime)
sudo systemctl reload nginx

# Or restart nginx (brief downtime)
sudo systemctl restart nginx

# Check nginx status
sudo systemctl status nginx

# Check nginx error logs
sudo tail -f /var/log/nginx/nasdaq_error.log
```

---

## Step 6: Check Logs

### Application Logs
```bash
# Flask application logs
sudo journalctl -u nasdaq-webapp -f

# Gunicorn access logs
tail -f /var/log/nasdaq/access.log

# Gunicorn error logs
tail -f /var/log/nasdaq/error.log
```

### Nginx Logs
```bash
# Nginx access log
sudo tail -f /var/log/nginx/nasdaq_access.log

# Nginx error log
sudo tail -f /var/log/nginx/nasdaq_error.log
```

---

## Step 7: Test from Browser

Open browser and test:
- http://www.masionias.com/
- http://www.masionias.com/db
- http://www.masionias.com/db/
- http://www.masionias.com/services
- http://www.masionias.com/services/

**All should work** with the updated nginx config.

---

## Quick Fix Checklist

```bash
# 1. Ensure Flask app is running
sudo systemctl start nasdaq-webapp
sudo systemctl enable nasdaq-webapp  # Auto-start on boot

# 2. Update nginx config
sudo cp ~/nasdaq/deployment/nginx.conf /etc/nginx/sites-available/nasdaq

# 3. Update YOUR_USERNAME in the config
sudo sed -i 's/YOUR_USERNAME/actual_username/g' /etc/nginx/sites-available/nasdaq

# 4. Test nginx config
sudo nginx -t

# 5. Reload nginx
sudo systemctl reload nginx

# 6. Test endpoints
curl http://127.0.0.1:8000/db/
curl http://www.masionias.com/db/
```

---

## Common Issues and Solutions

### Issue: "502 Bad Gateway"
**Cause:** Flask app not running or not listening on port 8000

**Solution:**
```bash
sudo systemctl restart nasdaq-webapp
sudo netstat -tlnp | grep 8000
```

### Issue: "404 Not Found" for /db or /services
**Cause:** Nginx config using wrong location blocks or root directive interfering

**Solution:** Use the updated nginx.conf with regex location blocks:
```nginx
location ~ ^/db(/.*)?$ {
    proxy_pass http://nasdaq_webapp;
    ...
}
```

### Issue: Static files (/, /404.html) not working
**Cause:** Wrong root path or files not deployed

**Solution:**
```bash
# Verify files exist
ls -la ~/nasdaq/static_html/

# Update root path in nginx config
root /home/yourusername/nasdaq/static_html;
```

### Issue: Changes not taking effect
**Cause:** Nginx config not reloaded

**Solution:**
```bash
sudo nginx -t && sudo systemctl reload nginx
```

---

## Configuration File Locations

- **Nginx config:** `/etc/nginx/sites-available/nasdaq`
- **Nginx symlink:** `/etc/nginx/sites-enabled/nasdaq`
- **Systemd service:** `/etc/systemd/system/nasdaq-webapp.service`
- **Gunicorn config:** `~/nasdaq/deployment/gunicorn.conf.py`
- **Static files:** `~/nasdaq/static_html/`
- **Application logs:** `/var/log/nasdaq/`

---

## Full Deployment Reset

If nothing works, try a complete reset:

```bash
# 1. Stop services
sudo systemctl stop nasdaq-webapp
sudo systemctl stop nginx

# 2. Update all configs
cd ~/nasdaq
git pull  # If using git
sudo cp deployment/nginx.conf /etc/nginx/sites-available/nasdaq
sudo cp deployment/nasdaq-webapp.service /etc/systemd/system/

# 3. Update YOUR_USERNAME in nginx config
sudo nano /etc/nginx/sites-available/nasdaq

# 4. Reload systemd
sudo systemctl daemon-reload

# 5. Start services
sudo systemctl start nasdaq-webapp
sudo systemctl start nginx

# 6. Check status
sudo systemctl status nasdaq-webapp
sudo systemctl status nginx

# 7. Test
curl http://127.0.0.1:8000/db/
curl http://www.masionias.com/db/
```

---

## Getting Help

If issues persist, collect diagnostic info:

```bash
# Create diagnostic report
cat << 'EOF' > ~/nasdaq-diagnostic.txt
=== System Info ===
$(uname -a)

=== Gunicorn Status ===
$(sudo systemctl status nasdaq-webapp)

=== Nginx Status ===
$(sudo systemctl status nginx)

=== Port 8000 ===
$(sudo netstat -tlnp | grep 8000)

=== Nginx Config Test ===
$(sudo nginx -t 2>&1)

=== Recent Application Logs ===
$(sudo journalctl -u nasdaq-webapp -n 20 --no-pager)

=== Recent Nginx Error Logs ===
$(sudo tail -20 /var/log/nginx/nasdaq_error.log)

=== Nginx Config ===
$(sudo cat /etc/nginx/sites-enabled/nasdaq)
EOF

cat ~/nasdaq-diagnostic.txt
```

Share this diagnostic report when asking for help.
