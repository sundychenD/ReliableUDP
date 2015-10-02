import java.io.*;
import java.net.*;
import java.nio.ByteBuffer;
import java.util.zip.CRC32;

/**
 * Created by chendi on 1/10/15.
 *
 *
 *
 * For initial meta data packet
 * [8 checksum, 4 index, 4 num of packets, up to 508 byte file name]
 *
 *
 * For normal data packet
 * [8 checksum, 4 index, up to 512 byte file name]
 */
public class FileReceiver {

    private static DatagramSocket sk;

    public static void main(String[] args) {
        if (args.length != 1) {
            System.err.println("Usage: FileReceiver <host_name>");
            System.exit(-1);
        }

        int port = Integer.parseInt(args[0]);
        try {
            sk = new DatagramSocket(port);
            FileReceiverEngine receiverEngine = new FileReceiverEngine(sk);
            receiverEngine.run();

        } catch (SocketException e) {
            System.out.println(e.getMessage());
            e.printStackTrace();
        } catch (IOException e) {
            System.out.println(e.getMessage());
            e.printStackTrace();
        }
    }
}


class FileReceiverEngine {

    DatagramSocket socket;

    public FileReceiverEngine(DatagramSocket sk) {
        this.socket = sk;
    }

    public void run() throws IOException {
        String fileName = null;
        int numOfPackets = 0;
        while (true) {
            FilePacket filePacket = new FilePacket(this.socket);
            filePacket.receivePkt();

            if (!filePacket.isCorrupted()) {
                if (filePacket.isMetaPackage()) {
                    // Get Meta packet, abandon it and send ACK -1
                    sendACK(-1, filePacket.getSocketAddress());
                    fileName = filePacket.getFileName();
                    numOfPackets = filePacket.getNumOfPackets();
                } else {
                    // Sender received meta ACK
                    // Cur packet is the first content packet, abandon it and send ACK -1
                    // Break meta packet check loop, go to content file receiving loop
                    sendACK(-1, filePacket.getSocketAddress());
                    break;
                }
            } else {
                // Meta packet corrupted
                sendACK(-2, filePacket.getSocketAddress());
            }
        }
        continueReceiveFile(fileName, numOfPackets);
    }

    private void continueReceiveFile(String fileName, int numPacket) {
        try {
            // Output file
            File file = new File(fileName);
            OutputStream outputStream = new FileOutputStream(file);

            FilePacket filePacket = new FilePacket(this.socket);;

            int receivedPacket = 0;
            int packetIndex = 0;
            while (true) {

                // Receive packet from socket
                filePacket.receivePkt();

                if (filePacket.pkt.getLength() < 8) {
                    System.out.println(" ===== Pkt too short");
                    continue;
                }
                packetIndex = filePacket.getPacketIndex();

                // Debug output
                // System.out.println("Received CRC:" + crc.getValue() + " Data:" + bytesToHex(data, pkt.getLength()));

                // Send Acknowledgement
                if (filePacket.isCorrupted()) {
                    System.out.println(" ===== Pkt corrupt -- " + packetIndex);
                    sendACK(receivedPacket, filePacket.getSocketAddress());
                } else {
                    if (packetIndex == (receivedPacket + 1)) {
                        // Receive correct packet
                        System.out.println("===== SUCCESS! Pkt received -- " + packetIndex);
                        sendACK(packetIndex, filePacket.getSocketAddress());

                        long bytesWrite = filePacket.getDataLength() - 12;
                        System.out.println("===== Write " + bytesWrite + "bytes");
                        outputStream.write(filePacket.data, 12, filePacket.getDataLength() - 12);

                        receivedPacket += 1;
                    } else {
                        // Packet out of order
                        System.out.println(" ===== Pkt out of order -- " + packetIndex);
                        sendACK(receivedPacket, filePacket.getSocketAddress());
                    }
                }

                // Close if received full length of packet
                if (packetIndex == numPacket) {
                    outputStream.close();
                }
            }

        } catch (SocketException e) {
            System.out.println(e.getMessage());
            e.printStackTrace();
        } catch (IOException e) {
            System.out.println(e.getMessage());
            e.printStackTrace();
        }
    }

    private void sendACK(int packetIndex, SocketAddress address) throws IOException {
        CRC32 crc = new CRC32();

        byte[] ack = new byte[12];
        ByteBuffer ackBuffer = ByteBuffer.wrap(ack);
        ackBuffer.clear();

        // Reserve space for checksum
        ackBuffer.putLong(0);
        ackBuffer.putInt(packetIndex);

        // Calculate checksum
        crc.reset();
        crc.update(ack, 8, 4);
        long ackCheckSum = crc.getValue();

        ackBuffer.rewind();
        ackBuffer.putLong(ackCheckSum);

        DatagramPacket ackPacket = new DatagramPacket(ack, 0, 12, address);
        this.socket.send(ackPacket);
    }
}

class FilePacket {

    private final int PACKET_LEN = 524;      // 12 byte header + up to 512 byte content
    public byte[] data;
    public ByteBuffer byteBuffer;
    public DatagramPacket pkt;
    private DatagramSocket socket;

    public FilePacket(DatagramSocket skt) {
        this.data = new byte[PACKET_LEN];
        this.byteBuffer = ByteBuffer.wrap(this.data);
        this.pkt = new DatagramPacket(this.data, this.data.length);
        this.pkt.setLength(this.data.length);
        this.socket = skt;
    }

    public void receivePkt() throws IOException {
        this.socket.receive(pkt);
    }

    public int getPacketIndex() {
        this.byteBuffer.rewind();
        this.byteBuffer.getLong();      // Skip checksum
        return this.byteBuffer.getInt();
    }

    public int getDataLength() {
        return this.pkt.getLength();
    }

    public SocketAddress getSocketAddress() {
        return this.pkt.getSocketAddress();
    }

    public boolean isCorrupted() {
        this.byteBuffer.rewind();
        long checksum = this.byteBuffer.getLong();

        CRC32 crc = new CRC32();
        crc.reset();
        crc.update(this.data, 8, this.pkt.getLength() - 8);
        return crc.getValue() != checksum;
    }

    public boolean isMetaPackage() {
        this.byteBuffer.rewind();
        this.byteBuffer.getLong();      // Skip checksum
        return this.byteBuffer.getInt() == -1;
    }

    /*
    * Method only used on meta packets
    *
    * return num of packets meta data
    * */
    public int getNumOfPackets() {
        this.byteBuffer.rewind();
        this.byteBuffer.getLong();      // Skip checksum
        this.byteBuffer.getInt();       // Skip index flag
        return this.byteBuffer.getInt();
    }

    /*
    * Method only used on meta packets
    *
    * return filename meta data
    * */
    public String getFileName() {
        this.byteBuffer.rewind();
        this.byteBuffer.getLong();      // Skip checksum
        this.byteBuffer.getInt();       // Skip index flag
        this.byteBuffer.getInt();       // Skip num of packets

        int fileNameCharLength = (this.pkt.getLength() - 16) / 2;
        String fileName = "";

        for (int i = 0; i < fileNameCharLength; i ++) {
            fileName += byteBuffer.getChar();
        }
        return fileName;
    }
}