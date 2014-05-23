require "bunny"







    def mergeSort(left, right)
      result = []
      until left.empty? || right.empty?
        if left.first <= right.first
          result << left.shift
        else
          result << right.shift
        end
      end
      return result + left + right
    end    



    def merge(mergeQueue,replyToQueue)
    puts "merge def"
        left=[]
        right=[]
    #while true do
     #   finish=false
       
        mergeQueue.subscribe(:block=>false,:ack=>true) do |delivery_info, properties, payload|
            puts "mergeSub"
            if left.empty?
                left=properties.headers["array"]
            else
                right=properties.headers["array"]
                puts properties.headers["array"].inspect
            end
    
            if not left.empty?   
                if  left.length ==properties.headers["finalCount"].to_i
                    replyToQueue.publish("RESULT",:persistent=>true,
                        :headers=>{
                        :array=>left},:correlation_id=>taskID)
                    channel.acknowledge(task_delivery_info.delivery_tag, false)
                    channel3.acknowledge(delivery_info.delivery_tag, false)
                   # finish=true
                end
            end
    
            if not right.empty?
                if  right.length ==properties.headers["finalCount"].to_i
                replyToQueue.publish("RESULT",:persistent=>true,
                    :headers=>{
                    :array=>right},:correlation_id=>taskID)
                    puts "FINISHED"
                    channel.acknowledge(task_delivery_info.delivery_tag, false)
                    channel3.acknowledge(delivery_info.delivery_tag, false)
                    finish=true
                end
           end

           if not left.empty? and not right.empty? 
               array=self.mergeSort(left,right)
               channel3.acknowledge(delivery_info.delivery_tag, false)
               puts array.length.to_s
               left=[]
               right=[]
    
              if array.length==properties.headers["finalCount"].to_i
                    replyToQueue.publish("RESULT",:persistent=>true,
                    :headers=>{
                    :array=>array},:correlation_id=>taskID)
                    puts "FINISHED"
                    channel.acknowledge(task_delivery_info.delivery_tag, false)
                    channel3.acknowledge(delivery_info.delivery_tag, false)
                    finish=true
 
             else
                 print "PUBLISHING MERGE"
                 mergeQueue.publish("MergeMessage",:persistent=>true,
                     :headers=>{
                     :taskID=>MergeQueue.name,
                     :replayTo=>replyToQueue.name,
                     :array=>array,
                     :finalCount=>finalCount})
                 channel3.acknowledge(delivery_info.delivery_tag, false)
             end
        end
  # ...
    end

    #break if finish
end

 

   


conn = Bunny.new
conn.start    
channel  = conn.create_channel
channel2  = conn.create_channel
channel3  = conn.create_channel
channel4  = conn.create_channel
newTaskExchange=channel.fanout("NewTask")

taskQueue=channel.queue("",:durable => true, :auto_delete => true,:exclusive => true)
taskQueue.bind(newTaskExchange)
channel.prefetch(1)
channel3.prefetch(2)
taskID=""
finalCount=""
customerID=(0...50).map { ('a'..'z').to_a[rand(26)] }.join
    
q=channel.queue(customerID,:durable => true, :auto_delete => true)
        
        
newWorker=channel.queue("NewWorkerQueue",:durable => true, :auto_delete => true)
newWorker.publish("Requesting signup,id "+customerID,:persistent=>true,:headers=>{
        :workerID=>customerID})
        
        noMessage=true
puts "Awaiting Task"

while noMessage do
    q.subscribe() do |task_delivery_info, task_properties, task_payload|
    customerID=task_properties.headers["replyTo"]
    taskID=task_properties.headers["taskID"]
    finalCount=task_properties.headers["finalCount"]
    noMessage=false
end
        
        
replyToQueue=channel2.queue(customerID,:durable => true, :auto_delete => true)
puts "Connected to replyQueue: "+customerID 
mergeQueue=channel3.queue(taskID,:durable => true)
puts "Connected to MergeQueue: "+taskID  
        # mergeQueue=MergeQueue

merge(mergeQueue,replyToQueue)         
     

    
taskQueue.subscribe(:block => true,:ack=>true,:exclusive => true) do |task_delivery_info, task_properties, task_payload|
    puts task_payload
    customerID=task_properties.headers["replyTo"]
    taskID=task_properties.headers["taskID"]
    finalCount=task_properties.headers["finalCount"]
    puts taskID


      replyToQueue=channel2.queue(customerID,:durable => true, :auto_delete => true)
            puts "Connected to replyQueue: "+customerID 
            mergeQueue=channel3.queue(taskID,:durable => true, :auto_delete => true)
            puts "Connected to MergeQueue: "+taskID  

            st=" "
            puts " [*] Waiting for logs. To exit press CTRL+C"
           
            merge(mergeQueue,replyToQueue)
            puts "ENDED"
        end
    
end

'''
while true do
delivery_info1=" "
delivery_tag1=" "
delivery_info2=" "
delivery_tag2=" "
begin
st=" "
   delivery_info1, properties1, payload1, = MergeQueue.pop

    if payload1!=nil 
        left=payload1.split(" ").map {|i| i.to_i}
        i+=1

       delivery_info2, properties2, payload2 = MergeQueue.pop

        if payload2!=nil 
            right=payload2.split(" ").map {|i| i.to_i}
 
        else right=[]
        end  
            array=merge(left,right)

            for number in array do
                st+=number.to_s+" "
            end 

            puts st+"\n"+array.length.to_s+"\n"
        #    
        MergeQueue.publish(st,:persistent=>true)

        #end
    end

    rescue Exception => e
#print st
      print e
      wait(2)
      channel.reject(delivery_info1.delivery_tag, true)
      channel.reject(delivery_info2.delivery_tag, true)
      channel.close
      conn.close
      break
 
    end

end
'''