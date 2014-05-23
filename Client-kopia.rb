require 'bunny'

class Client

    attr_accessor
        :channel
        :conn
        :replayToQueue
        :mergeQueue
        :newTaskExchange
        :@newTaskQueue
        :finalCount
        :mergeQueueName
        :replayToQueueName

    def initialize
        while true do
            begin 
                @conn = Bunny.new
                @conn.start
                @channel=@conn.create_channel
                @newTaskQueue==@channel.queue("NewTaskQueue",:durable => true, :auto_delete => true)
                break
            rescue Exception => e
                puts "Something went terribly wrong! I couldn't connect, the exception is:"
                print "=> "
                print e
                print " <=\n"
                for i in 0..5 do
                    print "Reconnecting in "
                    print (5-i).to_s
                    print  " seconds...\n"
                    sleep(1)
                end    
            end
        end
    end        

    def getRandomString
        return (0...50).map { ('a'..'z').to_a[rand(26)] }.join
    end    

    def readArrayFromFile(fileName)
        begin
            file_contents = File.read(fileName)
            array = file_contents.scan(/\d+/)
            array.collect! &:to_i
        rescue Exception=>e
            puts "Couldn't read from file, the exception is: "
                puts "=> "
                print e
                print " <=\n"
        end
        ensure
            return array
    end

    def saveArrayToFile(array,fileName)
        begin
            file=File.open(fileName, "w") { |io|  
                for number in array
                    io.print(number.to_s+" ")
                end    
            }
        rescue Exception=>e
            puts "Couldn't save. Exception is:"
            puts "=> "
            print e
            print " <=\n"
        end
    end        

    def start
        while true do 
            begin
                
                mergeQueue=channel.queue(mergeQueueName,:durable => true)
                replyToQueue=channel.queue(replayToQueueName,:durable => true, :auto_delete => true)
                puts "Succesfully Connected!"
                mergeQueue.purge()
                replyToQueue.purge()
                break
            rescue Exception=>e
                puts "Something went terribly wrong! I couldn't connect, the exception is:"
                print "=> "
                print e
                print " <=\n"
                for i in 0..5 do
                    print "Recconecting in "
                    print (5-i).to_s
                    print  " seconds...\n"
                    sleep(1)
                end    
            end
        end
        
        while true do

            choice=0
            numbers=Array.new

            while choice!=1 and choice!=2 do
                puts "\n---------------------------------------------------------------------------------"
                puts "Press 1) Use file as an input"
                puts "Press 2) Generate 10000 random numbers between 0 and 1000000 and write them to file"
                puts "---------------------------------------------------------------------------------"
                puts "\nPlease choose 1 or 2 \n"

                choice=gets.chomp
                if choice=='1'
                    puts "Enter FileName: "
                    fileName=gets.chomp
                    numbers=readArrayFromFile(fileName)
                    if numbers.is_a?(Array)
                        @finalCount=numbers.count
                        sendTask(numbers)
                        break
                    else next
                    end
                elsif choice=='2'

                    for i in 0..9999 do
                        random_number=Random.rand(1000000)
                        numbers.push(random_number)
                        File.open("Generated_Numbers.txt",'a'){|file| file.write(random_number)}
                        if i<9999
                            File.open("Generated_Numbers.txt",'a'){|file| file.write(" ")}
                        end    
                    end
                    @finalCount=numbers.count
                    puts "Generated 10000 random numbers and saved it in Generated_Numbers.txt!"
                    sendTask(numbers)
                    break
                end    
            end
        end

    end

    def sendTask(array)           
        @newTaskQueue.publish("NewTask",:persistent=>true,
            :headers=>{
            :taskID=>mergeQueue.name,
            :replyTo=>replyToQueue.name,
            :finalCount=>array.count
        },:message_id=>mergeQueue.name)

        puts "Send taskID: "+mergeQueue.name

        finalCount=array.count
        for number in array
            mergeQueue.publish("MergeMessage",:persistent=>true,
            :headers=>{
                    :taskID=>mergeQueue.name,
                    
                    :array=>[number],
                    :finalCount=>array.count},:reply_to=>replyToQueue.name,) 
        end

        puts "Awaiting reply..." 

        replyToQueue.subscribe(:block => true,:exclusive => true,:ack=>true) do |delivery_info, properties, payload|
            array=properties.headers["array"]
            puts "Recieved response!"
            puts "Do you want me to output sorted array?"
            
            choice=""

            while choice!='y' or choice !='n'
                puts "Please answer y/n"
                choice=gets.chomp
            end

            if choice=="y"
                puts array.inspect
                puts "===================="
                puts "Array length: " + array.length.to_s
            end    
            puts "Would you like to save the result to file?"
            choice=""

            while choice!='y' or choice !='n'
                puts "Please answer y/n"
                choice=gets.chomp
            end

            if choice=="y"
                choice=""
                
                while choice==""
                    puts "Please enter filename"
                    choice=gets.chomp
                end
                saveArrayToFile(array,choice)
            end
            
            puts "Finishing task...."
            puts "======================"        

            channel.acknowledge(delivey_info.delivery_tag,false)  
       end      
   end
end
