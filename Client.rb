require 'bunny'
require "SecureRandom"
class Client

    @@lastClient
    attr_accessor :channel,
        :conn,
        :taskID,
        :clientQueue,
        :mergeQueue,
        :taskInputQueue,
        :newTaskQueue,
        :finalCount,
        :clientID
    def self.lastClientID
        @@lastClientID
    end
    def self.lastClientID=(value)
        @@lastClientID=value
    end
    def initialize
        @clientID=SecureRandom.hex
        while true do
            begin 
                @conn = Bunny.new
                @conn.start
                @channel=@conn.create_channel
                @newTaskQueue=@channel.queue("NewTaskQueue",:durable => true)
                @clientQueue=@channel.queue(@clientID,:durable=>true,:auto_delete=>true)
               
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

            choice=0
            numbers=Array.new

            while choice!=1 and choice!=2 and choice!=3 do
                puts "\n---------------------------------------------------------------------------------"
                puts "Press 1) Use file as an input"
                puts "Press 2) Generate 256 random numbers between 0 and 1000000 and write them to file"
                puts "Press 3) For recovery mode"
                puts "---------------------------------------------------------------------------------"
                puts "\nPlease choose 1, 2 or 3\n"

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
                    f=File.new("Generated_Numbers.txt",'w')
                    f.close()
                    for i in 0..255 do
                        random_number=Random.rand(10000)
                        numbers.push(random_number)
                        File.open("Generated_Numbers.txt",'a'){|file| file.write(random_number)}
                        if i<255
                            File.open("Generated_Numbers.txt",'a'){|file| file.write(" ")}
                        end

                    end
                    @finalCount=numbers.count
                    puts "Generated 256 random numbers and saved it in Generated_Numbers.txt!"
                    sendTask(numbers)
                    break
                elsif choice=="3"
                    puts "Reading results from last queue..."
                    puts "Last client queue id is: "+@@lastClientID.to_s
                    puts Client.lastClientID

                    q=@channel.queue(@@lastClientID,:durable=>true,:auto_delete=>true)
                         if q.message_count!=0
                             delivey_info,properties,payload=q.pop
                             array=properties[:headers]["array"]
                             puts "Recieved response!"
                             puts "Do you want me to output sorted array?"
                         
                             choice=""

                             while choice!='y' and choice !='n'
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

                             while choice!='y' and choice !='n'
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
                              break
                         end
                       
                        
               
                end    
            end
        end

    end

    def sendTask(array)
        @taskID=SecureRandom.hex

        @@lastClientID=@taskID.to_s
        #sClient.lastClientID=@taskID
        puts "Setting lastClient id to: "+@@lastClientID.to_s
        @taskInputQueue=@channel.queue(@taskID,:durable=>true,:auto_delete=>true)
    
        puts "Send task with taskID: "+@taskID
        puts "Publishing task input..."
        finalCount=array.count
        
        for number in array
            @taskInputQueue.publish("MergeMessage",:persistent=>true,
            :headers=>{
                    :taskID=>@taskID,
                    :array=>[number],
                    :workerID=>0,
                    :finalCount=>array.count,
                    :reRouted=>false

                    },

                    :reply_to=>@clientID) 
        end

        puts "...published!"
        @newTaskQueue.publish("NewTaskQueue",:persistent=>true,
            :headers=>{
            :taskID=>@taskID,
            :finalCount=>array.count
        },
        :reply_to=>@clientID)

        puts "Awaiting reply..." 
        @@lastQueue=@clientQueue
        while true
            if @clientQueue.message_count!=0
                delivey_info,properties,payload=@clientQueue.pop
                array=properties[:headers]["array"]
                puts "Recieved response!"
                puts "Do you want me to output sorted array?"
            
                choice=""

                while choice!='y' and choice !='n'
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

                while choice!='y' and choice !='n'
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
                 break
            end
          
           
       end      
   end
end
client=Client.new
client.start