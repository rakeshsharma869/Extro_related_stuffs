import pandas as pd
import requests
import json
import logging
from typing import Dict, List, Any, Optional
from datetime import datetime
import re
import ast

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('product_upload.log'),
        logging.StreamHandler()
    ]
)

class ProductAPIUploader:
    def __init__(self, api_base_url: str = "http://localhost:8080/api", 
                 auth_token: Optional[str] = None):
        """
        Initialize the ProductAPIUploader
        
        Args:
            api_base_url: Base URL of the API
            auth_token: JWT token for authentication (if required)
        """
        self.api_base_url = api_base_url
        self.products_endpoint = f"{api_base_url}/products"
        self.session = requests.Session()
        
        # Set up headers
        self.session.headers.update({
            'Content-Type': 'application/json',
            'Accept': 'application/json'
        })
        
        # Add authentication header if token provided
        if auth_token:
            self.session.headers.update({
                'Authorization': f'Bearer {auth_token}'
            })
    
    def read_excel_data(self, file_path: str, sheet_name: str = 0) -> pd.DataFrame:
        """
        Read Excel file and return DataFrame
        
        Args:
            file_path: Path to the Excel file
            sheet_name: Sheet name or index (default: 0)
            
        Returns:
            pd.DataFrame: DataFrame containing the Excel data
        """
        try:
            df = pd.read_excel(file_path, sheet_name=sheet_name)
            logging.info(f"Successfully loaded Excel file: {file_path}")
            logging.info(f"Loaded {len(df)} rows of data")
            return df
        except Exception as e:
            logging.error(f"Error reading Excel file: {str(e)}")
            raise
    
    def parse_json_field(self, field_value: Any) -> Dict:
        """
        Parse JSON string fields from Excel
        
        Args:
            field_value: Value to parse as JSON
            
        Returns:
            Dict: Parsed JSON data or empty dict if parsing fails
        """
        if pd.isna(field_value) or field_value == '':
            return {}
        
        if isinstance(field_value, str):
            try:
                # Try to parse as JSON
                return json.loads(field_value)
            except json.JSONDecodeError:
                try:
                    # Try to evaluate as Python literal
                    return ast.literal_eval(field_value)
                except:
                    logging.warning(f"Could not parse JSON field: {field_value}")
                    return {}
        
        return field_value if isinstance(field_value, dict) else {}
    
    def parse_list_field(self, field_value: Any) -> List:
        """
        Parse list fields from Excel (comma-separated or JSON array)
        
        Args:
            field_value: Value to parse as list
            
        Returns:
            List: Parsed list or empty list if parsing fails
        """
        if pd.isna(field_value) or field_value == '':
            return []
        
        if isinstance(field_value, str):
            # Try JSON parsing first
            try:
                parsed = json.loads(field_value)
                return parsed if isinstance(parsed, list) else [parsed]
            except:
                # Fall back to comma-separated values
                return [item.strip() for item in field_value.split(',') if item.strip()]
        
        return field_value if isinstance(field_value, list) else []
    
    def parse_dimensions(self, dimensions_value: Any) -> Dict[str, float]:
        """
        Parse dimensions field from Excel
        
        Args:
            dimensions_value: Dimensions data (JSON string or dict)
            
        Returns:
            Dict: Dimensions with width, height, depth
        """
        default_dimensions = {"width": 0, "height": 0, "depth": 0}
        
        if pd.isna(dimensions_value) or dimensions_value == '':
            return default_dimensions
        
        try:
            if isinstance(dimensions_value, str):
                # Try to parse as JSON
                dimensions = json.loads(dimensions_value)
            else:
                dimensions = dimensions_value
            
            # Ensure all required keys exist
            result = default_dimensions.copy()
            if isinstance(dimensions, dict):
                result.update({
                    "width": float(dimensions.get("width", 0)),
                    "height": float(dimensions.get("height", 0)),
                    "depth": float(dimensions.get("depth", 0))
                })
            
            return result
        except:
            logging.warning(f"Could not parse dimensions: {dimensions_value}")
            return default_dimensions
    
    def parse_tags(self, tags_value: Any) -> List[Dict[str, str]]:
        """
        Parse tags field and convert to required format
        
        Args:
            tags_value: Tags data (comma-separated string or list)
            
        Returns:
            List[Dict]: List of tag objects
        """
        if pd.isna(tags_value) or tags_value == '':
            return []
        
        try:
            # Parse as list first
            tags_list = self.parse_list_field(tags_value)
            
            # Convert to required format
            return [{"tag": str(tag).strip()} for tag in tags_list if str(tag).strip()]
        except:
            logging.warning(f"Could not parse tags: {tags_value}")
            return []
    
    def parse_reviews(self, reviews_value: Any) -> List[Dict]:
        """
        Parse reviews field from Excel
        
        Args:
            reviews_value: Reviews data (JSON string or list)
            
        Returns:
            List[Dict]: List of review objects
        """
        if pd.isna(reviews_value) or reviews_value == '':
            return []
        
        try:
            reviews_data = self.parse_json_field(reviews_value)
            
            if isinstance(reviews_data, list):
                # Ensure each review has required fields
                formatted_reviews = []
                for review in reviews_data:
                    if isinstance(review, dict):
                        formatted_review = {
                            "rating": int(review.get("rating", 0)),
                            "comment": str(review.get("comment", "")),
                            "date": str(review.get("date", datetime.now().strftime("%Y-%m-%d"))),
                            "reviewerName": str(review.get("reviewerName", "Anonymous")),
                            "reviewerEmail": str(review.get("reviewerEmail", ""))
                        }
                        formatted_reviews.append(formatted_review)
                return formatted_reviews
            
            return []
        except:
            logging.warning(f"Could not parse reviews: {reviews_value}")
            return []
    
    
    def parse_images(self, images_value: Any, category: str = "", sku: str = "") -> List[Dict[str, str]]:
        """
        Parse images field and convert to required format
        
        Args:
            images_value: Images data (comma-separated URLs or list)
            
        Returns:
            List[Dict]: List of image objects
        """
        # if pd.isna(images_value) or images_value == '':
        #     return []
        try:
            images_list = self.parse_list_field(images_value)
            if len(images_list) == 0 and category and sku:
               safe_category = str(category).strip().replace(" ", "%20")
               safe_sku = str(sku).strip()
               image = f"https://raw.githubusercontent.com/rakeshsharma869/Extro_related_stuffs/refs/heads/master/All%20Tabs/{safe_category}/{safe_sku}.png"
               print("Fallback Image===>" + image)
               images_list.append(image)
            return [{"imageUrl": str(img).strip()} for img in images_list if str(img).strip()]
        except:
            logging.warning(f"Could not parse images: {images_value}")
            return []
    
    def parse_color_options(self, colors_value: Any) -> List[Dict[str, str]]:
        """
        Parse color options field and convert to required format
        
        Args:
            colors_value: Colors data (comma-separated string or list)
            
        Returns:
            List[Dict]: List of color option objects
        """
        if pd.isna(colors_value) or colors_value == '':
            return []
        
        try:
            colors_list = self.parse_list_field(colors_value)
            return [{"colorOption": str(color).strip()} for color in colors_list if str(color).strip()]
        except:
            logging.warning(f"Could not parse color options: {colors_value}")
            return []
    
    def parse_attachments(self, attachments_value: Any) -> List[Dict[str, str]]:
        """
        Parse attachments field from Excel
        
        Args:
            attachments_value: Attachments data (JSON string or list)
            
        Returns:
            List[Dict]: List of attachment objects
        """
        if pd.isna(attachments_value) or attachments_value == '':
            return []
        
        try:
            attachments_data = self.parse_json_field(attachments_value)
            
            if isinstance(attachments_data, list):
                formatted_attachments = []
                for attachment in attachments_data:
                    if isinstance(attachment, dict):
                        formatted_attachment = {
                            "attachmentType": str(attachment.get("attachmentType", "document")),
                            "attachmentLink": str(attachment.get("attachmentLink", ""))
                        }
                        formatted_attachments.append(formatted_attachment)
                return formatted_attachments
            
            return []
        except:
            logging.warning(f"Could not parse attachments: {attachments_value}")
            return []
    
    def parse_meta(self, meta_value: Any) -> Dict[str, str]:
        """
        Parse meta field from Excel
        
        Args:
            meta_value: Meta data (JSON string or dict)
            
        Returns:
            Dict: Meta object with barcode and qrCode
        """
        default_meta = {"barcode": "", "qrCode": ""}
        
        if pd.isna(meta_value) or meta_value == '':
            return default_meta
        
        try:
            meta_data = self.parse_json_field(meta_value)
            
            if isinstance(meta_data, dict):
                return {
                    "barcode": str(meta_data.get("barcode", "")),
                    "qrCode": str(meta_data.get("qrCode", ""))
                }
            
            return default_meta
        except:
            logging.warning(f"Could not parse meta: {meta_value}")
            return default_meta
    
    def transform_row_to_product(self, row: pd.Series) -> Dict[str, Any]:
        """
        Transform a DataFrame row to the required API format
        
        Args:
            row: pandas Series representing a row from the DataFrame
            
        Returns:
            Dict: Product data in API format
        """
        # Helper function to safely get values
        def safe_get(field, default=None, converter=None):
            value = row.get(field, default)
            if pd.isna(value):
                return default
            if converter:
                try:
                    return converter(value)
                except:
                    return default
            return value
        
        product = {
            "title": safe_get("Title", "", str),
            "description": safe_get("Description", "", str),
            "category": safe_get("Category", "", str),
            "subcategory": safe_get("SubCategory", "", str),
            "price": safe_get("Price", 0, float),
            "discountPercentage": safe_get("Discount Percentage", 0, float),
            "rating": safe_get("Rating", 0, float),
            "stock": safe_get("Stock", 0, int),
            "brand": safe_get("Brand", "", str),
            "sku": safe_get("Sku", "", str),
            "weight": safe_get("Weight", 0, float),
            "warrantyInformation": safe_get("Warranty Information", "", str),
            "shippingInformation": safe_get("Shipping Information", "", str),
            "availabilityStatus": safe_get("Availability Status", "In Stock", str),
            "returnPolicy": safe_get("Return Policy", "", str),
            "minimumOrderQuantity": safe_get("Minimum Order Quantity", 1, int),
            "thumbnail": safe_get("Thumbnail", "", str),
            "version": safe_get("Version", "1.0", str),
            "dimensions": self.parse_dimensions(row.get("Dimensions")),
            "tags": self.parse_tags(row.get("Tags")),
            "reviews": self.parse_reviews(row.get("Reviews")),
            "images": self.parse_images(row.get("Images"),row.get("Category"),row.get("Sku")),
            "colorOptions": self.parse_color_options(row.get("Color Options")),
            "attachments": self.parse_attachments(row.get("Attachments")),
            "meta": self.parse_meta(row.get("Meta"))
        }
        
        return product
    
    def post_product(self, product_data: Dict[str, Any]) -> bool:
        """
        Post product data to the API
        
        Args:
            product_data: Product data in API format
            
        Returns:
            bool: True if successful, False otherwise
        """
        try:
            response = self.session.post(
                self.products_endpoint,
                json=product_data,
                timeout=30
            )
            
            if response.status_code in [200, 201]:
                logging.info(f"Successfully posted product: {product_data.get('title', 'Unknown')}")
                return True
            else:
                logging.error(f"Failed to post product: {product_data.get('title', 'Unknown')} "
                            f"- Status: {response.status_code}, Response: {response.text}")
                return False
                
        except requests.exceptions.RequestException as e:
            logging.error(f"Network error posting product {product_data.get('title', 'Unknown')}: {str(e)}")
            return False
        except Exception as e:
            logging.error(f"Unexpected error posting product {product_data.get('title', 'Unknown')}: {str(e)}")
            return False
    
    def upload_products_from_excel(self, file_path: str, sheet_name: str = 0, 
                                 batch_size: int = 10) -> Dict[str, int]:
        """
        Upload all products from Excel file to API
        
        Args:
            file_path: Path to Excel file
            sheet_name: Sheet name or index
            batch_size: Number of products to process in each batch
            
        Returns:
            Dict: Statistics of the upload process
        """
        try:
            # Read Excel data
            df = self.read_excel_data(file_path, sheet_name)
            
            # Initialize statistics
            stats = {
                "total_products": len(df),
                "successful_uploads": 0,
                "failed_uploads": 0,
                "errors": []
            }
            
            # Process products in batches
            for i in range(0, len(df), batch_size):
                batch_df = df.iloc[i:i + batch_size]
                logging.info(f"Processing batch {i//batch_size + 1} ({len(batch_df)} products)")
                
                for index, row in batch_df.iterrows():
                    try:
                        # Transform row to product format
                        product_data = self.transform_row_to_product(row)
                        
                        # Validate required fields
                        if not product_data.get("title"):
                            logging.warning(f"Skipping row {index + 1}: Missing title")
                            stats["failed_uploads"] += 1
                            continue
                        
                        # Post to API
                        if self.post_product(product_data):
                            stats["successful_uploads"] += 1
                        else:
                            stats["failed_uploads"] += 1
                            
                    except Exception as e:
                        error_msg = f"Error processing row {index + 1}: {str(e)}"
                        logging.error(error_msg)
                        stats["failed_uploads"] += 1
                        stats["errors"].append(error_msg)
            
            # Log final statistics
            logging.info("="*50)
            logging.info("UPLOAD COMPLETED")
            logging.info(f"Total Products: {stats['total_products']}")
            logging.info(f"Successful Uploads: {stats['successful_uploads']}")
            logging.info(f"Failed Uploads: {stats['failed_uploads']}")
            logging.info(f"Success Rate: {(stats['successful_uploads']/stats['total_products']*100):.1f}%")
            logging.info("="*50)
            
            return stats
            
        except Exception as e:
            logging.error(f"Critical error in upload process: {str(e)}")
            raise

def main():
    """
    Main function to run the upload process
    """
    # Configuration
    API_BASE_URL = "http://localhost:8080/api"
    AUTH_TOKEN = None  # Add your JWT token here if authentication is required
    EXCEL_FILE_PATH = "extro.xls"  # Path to your Excel file
    SHEET_NAME = 0  # Sheet index or name
    
    # Initialize uploader
    uploader = ProductAPIUploader(
        api_base_url=API_BASE_URL,
        auth_token=AUTH_TOKEN
    )
    
    try:
        # Upload products
        stats = uploader.upload_products_from_excel(
            file_path=EXCEL_FILE_PATH,
            sheet_name=SHEET_NAME,
            batch_size=5  # Process 5 products at a time
        )
        
        # Print final results
        print("\n" + "="*50)
        print("FINAL RESULTS:")
        print(f"Total Products: {stats['total_products']}")
        print(f"Successful Uploads: {stats['successful_uploads']}")
        print(f"Failed Uploads: {stats['failed_uploads']}")
        
        if stats['errors']:
            print(f"\nErrors encountered: {len(stats['errors'])}")
            for error in stats['errors'][:5]:  # Show first 5 errors
                print(f"  - {error}")
            if len(stats['errors']) > 5:
                print(f"  ... and {len(stats['errors']) - 5} more errors")
        
    except Exception as e:
        logging.error(f"Failed to complete upload process: {str(e)}")
        print(f"Upload failed: {str(e)}")

if __name__ == "__main__":
    main()