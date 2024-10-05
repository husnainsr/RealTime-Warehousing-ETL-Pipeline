public class Controller implements Runnable {
    private SharedMemory buffer;
    private StreamData streamer;

    public Controller(SharedMemory buffer, StreamData streamer) {
        this.buffer = buffer;
        this.streamer = streamer;
    }

    @Override
    public void run() {
        try {
            while(!HybridJoin.stopper) {
                int bufferSize = buffer.getBufferSize();
                int newSleepTime;
//                System.out.println("Stream Size Currently "+bufferSize);
                if (bufferSize > buffer.streamSize * 0.8)
                    newSleepTime = 10;
                else if (bufferSize < buffer.streamSize * 0.5)
                    newSleepTime = 2;
                else
                    newSleepTime = 5;
                if (HybridJoin.stopper) {
                    break;
                }
//                System.out.println(newSleepTime);
                streamer.adjustRate(newSleepTime);
                Thread.sleep(2);
            }

        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }

}
