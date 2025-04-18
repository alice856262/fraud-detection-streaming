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

# Kafka Configuration
KAFKA_CONFIG = {
    'bootstrap_servers': 'localhost:9092',
    'fraud_alerts_topic': 'fraud_alerts',
    'top_products_topic': 'top_products'
}

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
    if topic == KAFKA_CONFIG['fraud_alerts_topic'] and st.session_state.fraud_consumer is None:
        try:
            st.session_state.fraud_consumer = KafkaConsumer(
                topic,
                bootstrap_servers=[KAFKA_CONFIG['bootstrap_servers']],
                auto_offset_reset='earliest',
                enable_auto_commit=True,
                value_deserializer=lambda x: json.loads(x.decode('utf-8')),
                consumer_timeout_ms=5000
            )
        except Exception as e:
            st.error(f"Failed to connect to Kafka: {str(e)}")
            return None
            
    elif topic == KAFKA_CONFIG['top_products_topic'] and st.session_state.product_consumer is None:
        try:
            st.session_state.product_consumer = KafkaConsumer(
                topic,
                bootstrap_servers=[KAFKA_CONFIG['bootstrap_servers']],
                auto_offset_reset='earliest',
                enable_auto_commit=True,
                value_deserializer=lambda x: json.loads(x.decode('utf-8')),
                consumer_timeout_ms=5000
            )
        except Exception as e:
            st.error(f"Failed to connect to Kafka: {str(e)}")
            return None
    
    return st.session_state.fraud_consumer if topic == KAFKA_CONFIG['fraud_alerts_topic'] else st.session_state.product_consumer

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
if time.time() - st.session_state.last_update.timestamp() > 10:
    update_fraud_data()
    update_product_data()
    st.session_state.last_update = datetime.now()

# Create containers for different sections
fraud_section = st.container()
product_section = st.container()

# Fraud Alerts Section
with fraud_section:
    st.header("Fraud Alerts")
    
    # Map at the top
    if st.session_state.fraud_data:
        fraud_df = pd.DataFrame(st.session_state.fraud_data)
        fraud_df[['lat', 'lon']] = fraud_df['location'].str.split(', ', expand=True).astype(float)
        
        fig_map = px.scatter_mapbox(
            fraud_df,
            lat='lat',
            lon='lon',
            hover_data=['timestamp', 'fraud_count', 'transaction_id'],
            color='fraud_count',
            size='fraud_count',
            zoom=2,
            height=400
        )
        fig_map.update_layout(mapbox_style="carto-positron")
        fig_map.update_layout(margin={"r":0,"t":0,"l":0,"b":0})
        st.plotly_chart(fig_map, use_container_width=True)
    
    # Chart and table side by side
    col1, col2 = st.columns(2)
    
    with col1:
        if st.session_state.fraud_data:
            fraud_df = pd.DataFrame(st.session_state.fraud_data)
            fraud_df['timestamp'] = pd.to_datetime(fraud_df['timestamp'])
            fraud_df = fraud_df.sort_values('timestamp')
            fraud_df['cumulative_fraud'] = fraud_df['fraud_count'].cumsum()
            
            fig = px.line(
                fraud_df,
                x='timestamp',
                y='cumulative_fraud',
                title='Cumulative Fraud Alerts Over Time'
            )
            fig.update_layout(
                xaxis_title='Time',
                yaxis_title='Cumulative Fraud Count',
                showlegend=False
            )
            st.plotly_chart(fig, use_container_width=True)
        else:
            st.info("Waiting for fraud alerts...")
    
    with col2:
        if st.session_state.fraud_data:
            latest_fraud = pd.DataFrame(st.session_state.fraud_data[-10:])
            st.dataframe(
                latest_fraud[['timestamp', 'fraud_count', 'transaction_id', 'location']],
                hide_index=True
            )
        else:
            st.info("Waiting for fraud alerts...")

# Top Products Section
with product_section:
    st.header("Top Products")
    
    col3, col4 = st.columns(2)
    
    with col3:
        if st.session_state.product_data:
            product_df = pd.DataFrame(st.session_state.product_data)
            product_df['timestamp'] = pd.to_datetime(product_df['timestamp'])
            
            # Group by product and sum quantities, then get top 10
            top_products = product_df.groupby('product_name')['quantity'].sum().reset_index()
            top_products = top_products.sort_values('quantity', ascending=False).head(10)
            
            fig = px.bar(
                top_products,
                x='product_name',
                y='quantity',
                title='Top 10 Products by Quantity'
            )
            fig.update_layout(
                xaxis_title='Product',
                yaxis_title='Quantity',
                showlegend=False
            )
            st.plotly_chart(fig, use_container_width=True)
        else:
            st.info("Waiting for product data...")
    
    with col4:
        if st.session_state.product_data:
            latest_products = pd.DataFrame(st.session_state.product_data[-10:])
            st.dataframe(
                latest_products[['timestamp', 'product_name', 'quantity']],
                hide_index=True
            )
        else:
            st.info("Waiting for product data...")

# Display latest update time
st.markdown("---")
st.markdown(f"Last updated: {st.session_state.last_update.strftime('%Y-%m-%d %H:%M:%S')}")

# Rerun after 10 seconds
time.sleep(10)
st.rerun() 