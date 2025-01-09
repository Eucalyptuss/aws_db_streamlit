# **Streamlit Weather Data Management Application**

This Streamlit project provides a web-based interface for managing weather data. It includes features for parsing weather data, updating and checking databases, and visualizing data from AWS RDS.

---

## **Project Structure**

```
.
├── .gitignore                  # Files to ignore in version control
├── env_information.txt         # Environment variable setup instructions
├── main.py                     # Main Streamlit application entry point
├── pages
│   ├── DB_Check.py             # Page to view and filter data from AWS RDS
│   ├── Parsing.py              # Page to parse and update weather data to AWS RDS
├── requirements.txt            # Python dependencies
```

---

## **Setup Instructions**

### **1. Clone the Repository**
```bash
git clone <repository_url>
cd <repository_directory>
```

### **2. Create a Virtual Environment**
```bash
python -m venv venv
source venv/bin/activate  # On Windows: venv\\Scripts\\activate
```

### **3. Install Dependencies**
```bash
pip install -r requirements.txt
```

### **4. Set Up Environment Variables**
1. Create a `.env` file in the root directory or follow the format in `env_information.txt`.
2. Add the following environment variables for AWS RDS access:
   ```env
   DB_HOST=your-rds-endpoint
   DB_USER=your-username
   DB_PASSWORD=your-password
   DB_NAME=your-database-name
   ```

---

## **Usage**

### **Run the Application**
```bash
streamlit run main.py
```

### **Pages**

#### **1. Parsing Weather Data**
- **File**: `pages/Parsing.py`
- **Description**: Parses weather data from the Meteostat API and updates it into AWS RDS (`past_weather` and `future_weather` tables).
- **Features**:
  - Parse historical weather data (past 7 days).
  - Parse forecast weather data (next 7 days).
  - Insert and update parsed data into AWS RDS.

#### **2. Check Database**
- **File**: `pages/DB_Check.py`
- **Description**: View and filter weather data from AWS RDS.
- **Features**:
  - Filter data by date range, region, and station name.
  - Display summarized and detailed weather data.
  - Visualize data trends and download data as CSV.

---

## **Development**

### **Add New Pages**
To add a new page:
1. Create a `.py` file under the `pages/` directory.
2. Add Streamlit code to the file.

### **Update Dependencies**
If you add new Python packages, update `requirements.txt`:
```bash
pip freeze > requirements.txt
```

---

## **Contributing**

Contributions are welcome! Please fork the repository and submit a pull request for review.

---

## **License**

This project is licensed under the MIT License.

---

If you encounter issues, feel free to create an issue in the repository or contact the project maintainer. 😊

