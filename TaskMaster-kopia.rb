require "Bunny"



conn = Bunny.new
conn.start
channel = conn.create_channel
newTaskExchange=channel.fanout("NewTask")

NewTaskQueue=channel.queue("NewTaskQueue",:durable => true, :auto_delete => true)

NewWorkerQueue=channel.queue("NewWorkerQueue",:durable => true, :auto_delete => true)

customerID=""
taskID=""
finalCount=""




NewTaskQueue.subscribe(:block=>true,:exclusive => true) do |task_delivery_info, task_properties, task_payload|
    puts task_payload
    customerID=task_properties.headers["replyTo"]
    taskID=task_properties.headers["taskID"]
    finalCount=task_properties.headers["finalCount"]
    
    newTaskExchange.publish("NewTask",:persistent=>true,
                :headers=>{
                    :taskID=>taskID,
                    :replyTo=>customerID,
                    :finalCount=>finalCount
                      
                },
                :message_id=>taskID 
                )


NewWorkerQueue.subscribe() do |delivery_info, properties, payload|
    workerQueue=channel.queue(properties.headers["workerID"],:durable => true, :auto_delete => true)
    workerQueue.publish("NewTask",:persistent=>true,
                    :headers=>{
                        :taskID=>taskID,
                        :replyTo=>customerID,
                        :finalCount=>finalCount
                          
                    },
                    :message_id=>taskID 
                    )

end
end
