import java.io.IOException;
import java.net.DatagramPacket;
import java.net.DatagramSocket;
import java.net.SocketException;
import java.nio.ByteBuffer;
import java.util.zip.CRC32;

/**
 * Created by chendi on 1/10/15.
 */
public class FileReceiver {

    private static final int PACKET_LEN = 512 + 12;

    public static void main(String[] args) {
        if (args.length != 1) {
            System.err.println("Usage: FileReceiver <host_name>");
            System.exit(-1);
        }

        int port = Integer.parseInt(args[0]);

        try {
            byte[] data = new byte[PACKET_LEN];
            ByteBuffer byteBuffer = ByteBuffer.wrap(data);
            CRC32 crc = new CRC32();

            DatagramSocket sk = new DatagramSocket(port);
            DatagramPacket pkt = new DatagramPacket(data, data.length);

            while (true) {
                pkt.setLength(data.length);
                sk.receive(pkt);
                if (pkt.getLength() < 8) {
                    System.out.println("Pkt too short");
                    continue;
                }

                byteBuffer.rewind();
                long chksum = byteBuffer.getLong();
                int packetIndex = byteBuffer.getInt();
                crc.reset();
                crc.update(data, 8, pkt.getLength() - 8);

                // Debug output
                // System.out.println("Received CRC:" + crc.getValue() + " Data:" + bytesToHex(data, pkt.getLength()));

                // Send Acknowledgement
                if (crc.getValue() != chksum) {
                    System.out.println("Pkt corrupt");
                } else {
                    System.out.println("Pkt " + packetIndex);

                    byte[] ack = new byte[4];
                    ByteBuffer ackBuffer = ByteBuffer.wrap(ack);
                    ackBuffer.putInt(packetIndex);
                    DatagramPacket ackPacket = new DatagramPacket(ack, 0, 4, pkt.getSocketAddress());
                    sk.send(ackPacket);
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
}
