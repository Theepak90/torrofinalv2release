import smtplib
import logging
import json
import sys
import os
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
from typing import List, Dict
import pymysql

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from config.azure_config import DB_CONFIG, DISCOVERY_CONFIG

logger = logging.getLogger(__name__)


def get_db_connection():
    return pymysql.connect(
        host=DB_CONFIG["host"],
        port=DB_CONFIG["port"],
        user=DB_CONFIG["user"],
        password=DB_CONFIG["password"],
        database=DB_CONFIG["database"],
        cursorclass=pymysql.cursors.DictCursor,
        charset='utf8mb4'
    )


def get_new_discoveries() -> List[Dict]:
    conn = None
    try:
        conn = get_db_connection()
        with conn.cursor() as cursor:
            sql = """
                SELECT id, file_metadata, storage_location, discovered_at, environment, data_source_type,
                       JSON_UNQUOTE(JSON_EXTRACT(storage_location, '$.path')) as storage_path
                FROM data_discovery
                WHERE discovered_at >= DATE_SUB(NOW(), INTERVAL 20 MINUTE)
                  AND notification_sent_at IS NULL
                ORDER BY discovered_at DESC
            """
            cursor.execute(sql)
            return cursor.fetchall()
    except Exception as e:
        logger.error('FN:get_new_discoveries error:{}'.format(str(e)))
        raise
    finally:
        if conn:
            conn.close()


def update_notification_status(discovery_ids: List[int], recipients: List[str]):
    if not discovery_ids:
        return
    
    conn = None
    try:
        conn = get_db_connection()
        with conn.cursor() as cursor:
            placeholders = ','.join(['%s'] * len(discovery_ids))
            sql = f"""
                UPDATE data_discovery
                SET notification_sent_at = NOW(),
                    notification_recipients = %s
                WHERE id IN ({placeholders})
            """
            recipients_json = json.dumps(recipients)
            cursor.execute(sql, [recipients_json] + discovery_ids)
            conn.commit()
    except Exception as e:
        logger.error('FN:update_notification_status discovery_ids:{} recipients:{} error:{}'.format(len(discovery_ids), len(recipients), str(e)))
        raise
    finally:
        if conn:
            conn.close()


def send_notification_email(discoveries: List[Dict], recipients: List[str]):
    if not discoveries or not recipients:
        logger.info('FN:send_notification_email discoveries_count:{} recipients_count:{}'.format(len(discoveries) if discoveries else 0, len(recipients) if recipients else 0))
        return
    
    try:
        smtp_server = DISCOVERY_CONFIG["smtp_server"]
        smtp_port = DISCOVERY_CONFIG["smtp_port"]
        smtp_user = DISCOVERY_CONFIG["smtp_user"]
        smtp_password = DISCOVERY_CONFIG["smtp_password"]
        
        msg = MIMEMultipart()
        msg['From'] = smtp_user
        msg['To'] = ', '.join(recipients)
        msg['Subject'] = f"New Data Discovered - {len(discoveries)} file(s)"
        
        body = f"""
        <html>
        <body>
            <h2>New Data Discovered</h2>
            <p>Hello Data Governors,</p>
            <p>The following {len(discoveries)} file(s) have been discovered:</p>
            <table border="1" cellpadding="5" cellspacing="0">
                <tr>
                    <th>File Name</th>
                    <th>Path</th>
                    <th>Environment</th>
                    <th>Data Source</th>
                    <th>Discovered At</th>
                    <th>Link</th>
                </tr>
        """
        
        frontend_url = os.getenv("FRONTEND_URL", "http://localhost:5162")
        
        for discovery in discoveries:
            discovery_id = discovery["id"]
            file_metadata = json.loads(discovery["file_metadata"]) if isinstance(discovery["file_metadata"], str) else discovery["file_metadata"]
            file_name = file_metadata.get("basic", {}).get("name", "unknown")
            
            # Extract storage_path from storage_location JSON or use the extracted field
            if "storage_path" in discovery and discovery["storage_path"]:
                storage_path = discovery["storage_path"]
            else:
                storage_location = json.loads(discovery["storage_location"]) if isinstance(discovery["storage_location"], str) else discovery["storage_location"]
                storage_path = storage_location.get("path", "unknown")
            
            environment = discovery.get("environment", "N/A")
            data_source_type = discovery.get("data_source_type", "N/A")
            discovered_at = discovery["discovered_at"].strftime("%Y-%m-%d %H:%M:%S")
            discovery_link = f"{frontend_url}/discovery?id={discovery_id}"
            
            body += f"""
                <tr>
                    <td>{file_name}</td>
                    <td>{storage_path}</td>
                    <td>{environment}</td>
                    <td>{data_source_type}</td>
                    <td>{discovered_at}</td>
                    <td><a href="{discovery_link}">View</a></td>
                </tr>
            """
        
        body += """
            </table>
            <p>Please review and approve/reject these discoveries.</p>
            <p>Best regards,<br>Torro Data Discovery System</p>
        </body>
        </html>
        """
        
        msg.attach(MIMEText(body, 'html'))
        
        if smtp_port == 465:
            server = smtplib.SMTP_SSL(smtp_server, smtp_port)
        else:
            server = smtplib.SMTP(smtp_server, smtp_port)
            server.starttls()
        
        server.login(smtp_user, smtp_password)
        server.send_message(msg)
        server.quit()
        
        logger.info('FN:send_notification_email discoveries_count:{} recipients_count:{}'.format(len(discoveries), len(recipients)))
        
        discovery_ids = [d["id"] for d in discoveries]
        update_notification_status(discovery_ids, recipients)
        
    except Exception as e:
        logger.error('FN:send_notification_email discoveries_count:{} recipients_count:{} error:{}'.format(len(discoveries), len(recipients), str(e)))
        raise


def notify_new_discoveries():
    try:
        discoveries = get_new_discoveries()
        if discoveries:
            recipients = [email.strip() for email in DISCOVERY_CONFIG["notification_recipients"] if email.strip()]
            if recipients:
                send_notification_email(discoveries, recipients)
            else:
                logger.warning('FN:notify_new_discoveries discoveries_count:{} recipients_configured:{}'.format(len(discoveries), bool(recipients)))
        else:
            logger.info('FN:notify_new_discoveries discoveries_count:{}'.format(0))
    except Exception as e:
        logger.error('FN:notify_new_discoveries error:{}'.format(str(e)))
        raise
