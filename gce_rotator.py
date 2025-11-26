import subprocess
import time
import requests
import logging

class GCERotator:
    def __init__(self):
        self.metadata_url = "http://metadata.google.internal/computeMetadata/v1/instance"
        self.headers = {"Metadata-Flavor": "Google"}
        
        # Tự động lấy thông tin VM hiện tại
        try:
            self.instance_name = self._get_metadata("name")
            # Zone trả về dạng 'projects/123/zones/us-central1-a', cần cắt lấy phần cuối
            self.zone = self._get_metadata("zone").split("/")[-1]
            logging.info(f"Initialized Rotator for VM: {self.instance_name} in {self.zone}")
        except Exception as e:
            logging.error(f"Failed to fetch metadata. Are you running inside GCE? Error: {e}")
            # Fallback or re-raise depending on strictness. 
            # For now, we log error but allow init (methods will fail later).
            self.instance_name = None
            self.zone = None

    def _get_metadata(self, path):
        response = requests.get(f"{self.metadata_url}/{path}", headers=self.headers, timeout=2)
        response.raise_for_status()
        return response.text

    def rotate_ip(self):
        if not self.instance_name or not self.zone:
            logging.error("❌ Cannot rotate IP: Missing instance metadata.")
            return False

        logging.warning("🔄 STARTING IP ROTATION SEQUENCE...")
        
        # Tên config mạng mặc định của GCE thường là "external-nat"
        # Nếu bạn custom network, hãy đổi tên này
        access_config_name = "external-nat"

        cmd_delete = [
            "gcloud", "compute", "instances", "delete-access-config", 
            self.instance_name, 
            f"--zone={self.zone}", 
            f"--access-config-name={access_config_name}",
            "--quiet"
        ]

        cmd_add = [
            "gcloud", "compute", "instances", "add-access-config", 
            self.instance_name, 
            f"--zone={self.zone}", 
            f"--access-config-name={access_config_name}",
            "--quiet"
        ]

        try:
            # 1. Xóa IP cũ
            logging.info("Deleting old IP...")
            subprocess.run(cmd_delete, check=True, capture_output=True)
            
            # 2. Thêm IP mới
            logging.info("Requesting new IP...")
            subprocess.run(cmd_add, check=True, capture_output=True)
            
            # 3. Chờ mạng ổn định (Rất quan trọng)
            logging.info("IP changed. Waiting 10s for network stabilization...")
            time.sleep(10) 
            
            logging.info("✅ IP ROTATION COMPLETED.")
            return True

        except subprocess.CalledProcessError as e:
            logging.error(f"❌ FAILED to rotate IP. Gcloud Error: {e.stderr.decode()}")
            return False
        except Exception as ex:
            logging.error(f"❌ FAILED to rotate IP. Exception: {ex}")
            return False

# Test thử (chạy trực tiếp file này trên VM để test)
if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    rotator = GCERotator()
    rotator.rotate_ip()
