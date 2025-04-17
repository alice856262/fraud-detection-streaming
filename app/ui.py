import streamlit as st
import plotly.express as px
import pandas as pd
from datetime import datetime, timedelta
import time
from kafka import KafkaConsumer
import json

# Page config
st.set_page_config(
    page_title="Real-time Fraud Detection Dashboard",
    page_icon="🔍",
    layout="wide"
)

# Title
st.title("Real-time Fraud Detection Dashboard")

# Initialize session state
if 'fraud_data' not in st.session_state:
    st.session_state.fraud_data = []
if 'product_data' not in st.session_state:
    st.session_state.product_data = []
if 'last_update' not in st.session_state:
    st.session_state.last_update = datetime.now() - timedelta(seconds=10)
if 'fraud_consumer' not in st.session_state:
    st.session_state.fraud_consumer = None
if 'product_consumer' not in st.session_state:
    st.session_state.product_consumer = None

# Initialize Kafka consumer
def get_kafka_consumer(topic):
    if topic == 'fraud_alerts' and st.session_state.fraud_consumer is None:
        st.session_state.fraud_consumer = KafkaConsumer(
            topic,
            bootstrap_servers=['localhost:9092'],
            auto_offset_reset='earliest',
            enable_auto_commit=True,
            value_deserializer=lambda x: json.loads(x.decode('utf-8')),
            consumer_timeout_ms=5000
        )
    elif topic == 'top_products' and st.session_state.product_consumer is None:
        st.session_state.product_consumer = KafkaConsumer(
            topic,
            bootstrap_servers=['localhost:9092'],
            auto_offset_reset='earliest',
            enable_auto_commit=True,
            value_deserializer=lambda x: json.loads(x.decode('utf-8')),
            consumer_timeout_ms=5000
        )
    
    return st.session_state.fraud_consumer if topic == 'fraud_alerts' else st.session_state.product_consumer

# Create two columns for the dashboard
col1, col2 = st.columns(2)

# Function to update fraud data
def update_fraud_data():
    try:
        consumer = get_kafka_consumer('fraud_alerts')
        if consumer is None:
            return
            
        messages = consumer.poll(timeout_ms=5000)
        if not messages:
            return
        
        for tp, msgs in messages.items():
            for message in msgs:
                st.session_state.fraud_data.append({
                    'timestamp': datetime.now(),
                    'fraud_count': message.value.get('fraud_count', 0),
                    'transaction_id': message.value.get('transaction_id', 'N/A'),
                    'location': f"{message.value.get('shipment_location_lat', 0)}, {message.value.get('shipment_location_long', 0)}"
                })
        
        # Keep only last 100 records
        if len(st.session_state.fraud_data) > 100:
            st.session_state.fraud_data = st.session_state.fraud_data[-100:]
    except Exception as e:
        st.error(f"Error reading fraud alerts: {str(e)}")

# Function to update product data
def update_product_data():
    try:
        consumer = get_kafka_consumer('top_products')
        if consumer is None:
            return
            
        messages = consumer.poll(timeout_ms=5000)
        if not messages:
            return
        
        for tp, msgs in messages.items():
            for message in msgs:
                st.session_state.product_data.append({
                    'timestamp': datetime.now(),
                    'product_id': message.value.get('product_id', 'N/A'),
                    'product_name': message.value.get('productDisplayName', 'Unknown Product'),
                    'quantity': message.value.get('total_quantity', 0)
                })
        
        # Keep only last 100 records
        if len(st.session_state.product_data) > 100:
            st.session_state.product_data = st.session_state.product_data[-100:]
    except Exception as e:
        st.error(f"Error reading product data: {str(e)}")

# Update data every 10 seconds
if (datetime.now() - st.session_state.last_update).total_seconds() >= 10:
    update_fraud_data()
    update_product_data()
    st.session_state.last_update = datetime.now()

with col1:
    st.header("Fraud Alerts")
    
    # Create a line chart for fraud alerts
    if st.session_state.fraud_data:
        df_fraud = pd.DataFrame(st.session_state.fraud_data)
        # Keep only last hour of data
        df_fraud = df_fraud[df_fraud['timestamp'] > datetime.now() - timedelta(hours=1)]
        
        # Aggregate fraud counts by timestamp
        df_fraud_agg = df_fraud.groupby('timestamp')['fraud_count'].sum().reset_index()
        # Calculate cumulative sum of fraud counts
        df_fraud_agg['total_fraud_count'] = df_fraud_agg['fraud_count'].cumsum()
        
        # Line chart for fraud counts
        fig_fraud = px.line(df_fraud_agg, 
                          x='timestamp', 
                          y='total_fraud_count',
                          title='Cumulative Fraud Alerts Over Time')
        fig_fraud.update_xaxes(title='Time')
        fig_fraud.update_yaxes(title='Total Fraud Count')
        st.plotly_chart(fig_fraud, use_container_width=True)
        
        # Create DataFrame for map with proper lat/lon columns
        df_fraud['lat'] = df_fraud['location'].apply(lambda x: float(x.split(',')[0]))
        df_fraud['lon'] = df_fraud['location'].apply(lambda x: float(x.split(',')[1]))
        
        # Create map of fraud locations
        fig_map = px.scatter_mapbox(df_fraud,
                                  lat='lat',
                                  lon='lon',
                                  hover_data=['timestamp', 'fraud_count', 'transaction_id'],
                                  zoom=4,
                                  title='Fraud Alert Locations')
        
        fig_map.update_layout(
            mapbox_style='carto-positron',  # Use a light map style
            mapbox=dict(
                center=dict(lat=df_fraud['lat'].mean(), 
                          lon=df_fraud['lon'].mean())
            ),
            height=400  # Set a fixed height for the map
        )
        st.plotly_chart(fig_map, use_container_width=True)
        
        # Display latest fraud alerts
        st.subheader("Latest Fraud Alerts")
        st.dataframe(
            df_fraud.tail(5)[['timestamp', 'fraud_count', 'transaction_id', 'location']],
            hide_index=True  # Hide the index column
        )
    else:
        st.info("Waiting for fraud alerts...")

with col2:
    st.header("Top Products")
    
    # Create a bar chart for top products
    if st.session_state.product_data:
        df_products = pd.DataFrame(st.session_state.product_data)
        # Keep only last hour of data
        df_products = df_products[df_products['timestamp'] > datetime.now() - timedelta(hours=1)]
        
        # Group by product and sum quantities
        df_products_grouped = df_products.groupby('product_name')['quantity'].sum().reset_index()
        df_products_grouped = df_products_grouped.sort_values('quantity', ascending=False).head(10)
        
        fig_products = px.bar(df_products_grouped, x='product_name', y='quantity',
                            title='Top 10 Products by Quantity')
        st.plotly_chart(fig_products, use_container_width=True)
        
        # Display latest product data
        st.subheader("Latest Product Updates")
        st.dataframe(
            df_products.tail(5)[['timestamp', 'product_name', 'quantity']],
            hide_index=True  # Hide the index column
        )
    else:
        st.info("Waiting for product data...")

# Add a footer with last update time
st.markdown("---")
st.markdown(f"Real-time Fraud Detection System | Last updated: {st.session_state.last_update.strftime('%Y-%m-%d %H:%M:%S')}")

# Rerun after 10 seconds
time.sleep(10)
st.rerun() 