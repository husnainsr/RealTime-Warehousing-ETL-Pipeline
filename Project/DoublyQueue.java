class Node {
    String data;
    Node prev, next;

    public Node(String data) {
        this.data = data;
        this.prev = null;
        this.next = null;
    }
}
public class DoublyQueue{
    Node front;
    Node rear;
    int totalSize;
    int currentSize;

    public  DoublyQueue(int size)
    {
        this.front=null;
        this.rear=null;
        this.totalSize=size;
        this.currentSize=0;
    }

    void enque(String data)
    {
            Node tempNode = new Node(data);
            //if queue is empty
            if(this.front==null)
            {
                this.front=tempNode;
                this.rear=tempNode;
                currentSize++;
            }
            else{                   //add at the back
                rear.next=tempNode;
                tempNode.prev=rear;
                rear=tempNode;
                currentSize++;
            }

    }

    String deque()
    {
        if(front==null) //if qeueu empty
            return null;

        Node temp =front;
        front=front.next;

        if(front==null)
        {
            rear=null;
        }
        else {
            front.prev=null;
        }
        currentSize--;
        return temp.data;
    }


    String getFront()
    {
        if (front == null)
        {
            return null;
        }
        return front.data;
    }

    String getRear()
    {
        if (rear == null)
        {
            return null;
        }
        return rear.data;
    }

    void DisplayQueu()
    {
        Node iterator = front;
        System.out.print("Queue: ");
        while (iterator != null) {
            System.out.print(iterator.data + " ");
            iterator = iterator.next;
        }
    }

    int delete(String value) {
        Node current = front;
        int deleteCount = 0;

        while (current != null) {
            if (current.data.equals(value)) {
                deleteCount++;
//                System.out.println(current.data);
                if (current.prev != null)
                {
                    current.prev.next = current.next;
                }
                else {
                    front = current.next; //If the node to delete is the front node
                }

                if (current.next != null)
                {
                    current.next.prev = current.prev;
                }
                else {
                    rear = current.prev; //If the node to delete is the rear node
                }

                currentSize--;
            }

            current = current.next; // Move to the next node
        }

        return deleteCount;
    }
}

