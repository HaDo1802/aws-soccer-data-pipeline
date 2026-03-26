#!/bin/bash

FUNCTION_NAME=$1
HANDLER_FILE=$2

BUILD_DIR=build_$FUNCTION_NAME

echo "Building $FUNCTION_NAME..."

rm -rf $BUILD_DIR
mkdir $BUILD_DIR

# Copy code
cp -r src $BUILD_DIR/
cp -r utils $BUILD_DIR/
cp lambda_deployment/$HANDLER_FILE $BUILD_DIR/
cp requirements.txt $BUILD_DIR/

# Install dependencies (clean)
pip install -r requirements.txt -t $BUILD_DIR/ --no-cache-dir

# Remove junk
find $BUILD_DIR -name "__pycache__" -exec rm -r {} +
find $BUILD_DIR -name "*.pyc" -delete

# Zip
cd $BUILD_DIR
zip -r ../$FUNCTION_NAME.zip .
cd ..

echo "$FUNCTION_NAME.zip ready"