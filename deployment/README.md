# Deployment Guide for Nasdaq Database Query Tool

This guide explains how to deploy the Nasdaq Database Query Tool to a production server at `http://www.masionias.com/db/`.

## Prerequisites

- Ubuntu/Debian Linux server with root or sudo access
- Python 3.9+
- Nginx web server
- MySQL 8.0 database (running via Docker or native)
- Domain name configured (masionias.com)

## Deployment Steps

### 1. Prepare the Server

```bash
# Update system packages
sudo apt update && sudo apt upgrade -y

# Install required packages
sudo apt install -y python3 python3-pip python3-venv nginx git

# Install Docker (if not already installed)
curl -fsSL https://get.docker.com -o get-docker.sh
sudo sh get-docker.sh
sudo usermod -aG docker $USER
```

### 2. Clone the Repository

```bash
# Create application directory
sudo mkdir -p /var/www/nasdaq
sudo chown $USER:$USER /var/www/nasdaq

# Clone the repository
cd /var/www
git clone <your-repo-url> nasdaq
cd nasdaq
```

### 3. Set Up Python Environment

```bash
# Create virtual environment
python3 -m venv .venv

# Activate virtual environment
source .venv/bin/activate

# Install dependencies
pip install -r requirements.txt
```

### 4. Configure the Application

```bash
# Copy and configure config.yaml
cp config.yaml.example config.yaml  # If you have an example file
nano config.yaml

# Set database credentials
# Make sure the database section matches your MySQL setup
```

### 5. Start MySQL Database

```bash
# If using Docker
docker-compose up -d

# Initialize database schema
python src/init_db.py
```

### 6. Set Up Log Directory

```bash
# Create log directory
sudo mkdir -p /var/log/nasdaq

# Set appropriate permissions
sudo chown www-data:www-data /var/log/nasdaq
sudo chmod 755 /var/log/nasdaq
```

### 7. Install Systemd Service

```bash
# Copy systemd service file
sudo cp deployment/nasdaq-webapp.service /etc/systemd/system/

# Edit the service file to match your setup
sudo nano /etc/systemd/system/nasdaq-webapp.service

# Update these values:
# - User and Group (e.g., your username or www-data)
# - WorkingDirectory (should be /var/www/nasdaq)
# - ExecStart path (should point to your venv)

# Reload systemd
sudo systemctl daemon-reload

# Enable service to start on boot
sudo systemctl enable nasdaq-webapp

# Start the service
sudo systemctl start nasdaq-webapp

# Check status
sudo systemctl status nasdaq-webapp
```

### 8. Configure Nginx

```bash
# Copy nginx configuration
sudo cp deployment/nginx.conf /etc/nginx/sites-available/nasdaq

# Create symbolic link to enable the site
sudo ln -s /etc/nginx/sites-available/nasdaq /etc/nginx/sites-enabled/

# Test nginx configuration
sudo nginx -t

# If test passes, reload nginx
sudo systemctl reload nginx
```

### 9. Configure Firewall (if using UFW)

```bash
# Allow HTTP and HTTPS traffic
sudo ufw allow 'Nginx Full'

# Check firewall status
sudo ufw status
```

### 10. Set Up SSL Certificate (Optional but Recommended)

```bash
# Install Certbot
sudo apt install -y certbot python3-certbot-nginx

# Obtain SSL certificate
sudo certbot --nginx -d www.masionias.com -d masionias.com

# Follow the prompts to configure HTTPS
# Certbot will automatically update your nginx configuration

# Test automatic renewal
sudo certbot renew --dry-run
```

## Verification

After deployment, verify the installation:

1. **Check the service status:**
   ```bash
   sudo systemctl status nasdaq-webapp
   ```

2. **Check application logs:**
   ```bash
   sudo tail -f /var/log/nasdaq/error.log
   sudo tail -f /var/log/nasdaq/access.log
   ```

3. **Check nginx logs:**
   ```bash
   sudo tail -f /var/log/nginx/nasdaq_error.log
   sudo tail -f /var/log/nginx/nasdaq_access.log
   ```

4. **Test the application:**
   - Visit `http://www.masionias.com/db/` in your browser
   - Try executing a sample query: `SELECT * FROM companies LIMIT 10`

## Troubleshooting

### Service Won't Start

```bash
# Check service logs
sudo journalctl -u nasdaq-webapp -n 50 --no-pager

# Check if port 8000 is already in use
sudo netstat -tulpn | grep 8000

# Verify Python environment
/var/www/nasdaq/.venv/bin/python --version
/var/www/nasdaq/.venv/bin/pip list
```

### Database Connection Issues

```bash
# Check if MySQL is running
docker ps  # If using Docker
sudo systemctl status mysql  # If using native MySQL

# Test database connection
mysql -h 127.0.0.1 -u nasdaq_user -p nasdaq

# Verify config.yaml settings
cat /var/www/nasdaq/config.yaml
```

### Nginx 502 Bad Gateway

```bash
# Check if gunicorn is running
sudo systemctl status nasdaq-webapp

# Check if application is listening on port 8000
sudo netstat -tulpn | grep 8000

# Check nginx error logs
sudo tail -f /var/log/nginx/nasdaq_error.log
```

### Permission Issues

```bash
# Fix ownership
sudo chown -R www-data:www-data /var/www/nasdaq

# Fix log directory permissions
sudo chown -R www-data:www-data /var/log/nasdaq
sudo chmod -R 755 /var/log/nasdaq
```

## Maintenance

### Updating the Application

```bash
# Stop the service
sudo systemctl stop nasdaq-webapp

# Pull latest changes
cd /var/www/nasdaq
git pull

# Activate virtual environment
source .venv/bin/activate

# Update dependencies
pip install -r requirements.txt

# Restart the service
sudo systemctl start nasdaq-webapp

# Check status
sudo systemctl status nasdaq-webapp
```

### Restarting Services

```bash
# Restart the Flask application
sudo systemctl restart nasdaq-webapp

# Restart Nginx
sudo systemctl restart nginx

# Restart both
sudo systemctl restart nasdaq-webapp nginx
```

### Viewing Logs

```bash
# Application logs
sudo tail -f /var/log/nasdaq/error.log
sudo tail -f /var/log/nasdaq/access.log

# Systemd service logs
sudo journalctl -u nasdaq-webapp -f

# Nginx logs
sudo tail -f /var/log/nginx/nasdaq_error.log
sudo tail -f /var/log/nginx/nasdaq_access.log
```

## Security Considerations

1. **Database Credentials**: Ensure `config.yaml` is not readable by others:
   ```bash
   chmod 600 /var/www/nasdaq/config.yaml
   ```

2. **Firewall**: Only allow necessary ports (80, 443, 22)

3. **SSL/TLS**: Always use HTTPS in production (via Let's Encrypt)

4. **Regular Updates**: Keep the system and dependencies updated:
   ```bash
   sudo apt update && sudo apt upgrade
   pip install --upgrade -r requirements.txt
   ```

5. **Database Access**: The application only allows SELECT queries, but still limit database user permissions

6. **Monitoring**: Set up monitoring for the application and server resources

## Performance Tuning

### Gunicorn Workers

Edit `/etc/systemd/system/nasdaq-webapp.service` to adjust the number of workers:

```ini
--workers 4  # Adjust based on CPU cores (2 * num_cores + 1)
```

### Nginx Caching

Add caching to nginx configuration for better performance (edit `/etc/nginx/sites-available/nasdaq`).

### Database Connection Pool

The application uses SQLAlchemy connection pooling by default. Adjust if needed in `src/db.py`.

## Backup

### Database Backup

```bash
# Backup MySQL database
docker exec nasdaq_mysql mysqldump -u nasdaq_user -p nasdaq > backup_$(date +%Y%m%d).sql

# Or if using native MySQL
mysqldump -u nasdaq_user -p nasdaq > backup_$(date +%Y%m%d).sql
```

### Application Backup

```bash
# Backup configuration
cp /var/www/nasdaq/config.yaml ~/backups/config.yaml.$(date +%Y%m%d)

# Backup entire application
tar -czf nasdaq_backup_$(date +%Y%m%d).tar.gz /var/www/nasdaq
```

## Support

For issues or questions, check:
- Application logs in `/var/log/nasdaq/`
- Nginx logs in `/var/log/nginx/`
- Systemd logs via `journalctl -u nasdaq-webapp`
