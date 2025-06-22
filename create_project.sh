#!/bin/bash

# Define project directory name
PROJECT_DIR="foodstory_bill_importer"

echo "Creating project directory: $PROJECT_DIR"

# Create the main project directory
mkdir -p "$PROJECT_DIR"

# Change to the project directory
cd "$PROJECT_DIR" || exit

echo "Creating core files..."

# Create core files
touch app.py
touch db_manager.py
touch requirements.txt

echo "Creating 'data' subdirectory..."

# Create data directory
mkdir -p data

echo "Adding initial content to requirements.txt..."

# Add initial content to requirements.txt
cat <<EOL > requirements.txt
streamlit
pandas
sqlite3
EOL

echo "Project '$PROJECT_DIR' structure created successfully!"
echo "You can now navigate into the directory: cd $PROJECT_DIR"
echo "Then install dependencies: pip install -r requirements.txt"
echo "And run the Streamlit app: streamlit run app.py"