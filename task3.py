import json
import pika
import os
import numpy as np
import pandas as pd
import matplotlib.pyplot as plt
from ml_engine import MLPredictor


def step2_timestamp_format_convert(data):
    '''timestamp format convert'''
    print(data, flush=True)
    return data


def step3_plot_daily_data(df_data):
    '''plot daily data'''
    # Initialize a canvas
    plt.figure(figsize=(12, 9), dpi=200)
    # Plot data into canvas
    plt.plot(df_data["Time"], df_data["Value"], color="#FF3B1D", marker='.', linestyle="-")
    plt.title("Averaging Value for demonstration")
    plt.xlabel("DateTime")
    plt.ylabel("Value")
    plt.xticks(rotation=270)
    plt.xticks(fontsize=8)
    # Save as file
    plt.savefig("figure1.png")
    # Directly display
    # plt.show()

def step4_model_train_predict(df_data):
    # model
    df_data = df_data.rename(columns = {'Time' : 'Timestamp'})
    predictor = MLPredictor(df_data)
    predictor.train()
    result = predictor.predict()
    return predictor, result

def step5_draw_picture(predictor, result):
    # draw picture
    fig = predictor.plot_result(result)
    fig.savefig(os.path.join("./", "output.png"))
    # fig.show()

def step1_grab_data_from_rabbitMQ():
    rabbitmq_ip = "localhost"
    rabbitmq_port = 5672
    # Queue name
    rabbitmq_queque = "CSC8112"

    def callback(ch, method, properties, body):
        print(f"Got message from producer msg: {json.loads(body)}")
        data=pd.DataFrame.from_dict(json.loads(body))
        pm25_df = step2_timestamp_format_convert(data)
        step3_plot_daily_data(pm25_df)
        predictor, result = step4_model_train_predict(pm25_df)
        step5_draw_picture(predictor, result)
        

    # Connect to RabbitMQ service with timeout 1min
    connection = pika.BlockingConnection(
        pika.ConnectionParameters(host=rabbitmq_ip, port=rabbitmq_port, socket_timeout=60))
    channel = connection.channel()
    # Declare a queue
    channel.queue_declare(queue=rabbitmq_queque)

    channel.basic_consume(queue=rabbitmq_queque,
                          auto_ack=True,
                          on_message_callback=callback)

    channel.start_consuming()




if __name__ == "__main__":
    step1_grab_data_from_rabbitMQ()
    