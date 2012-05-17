package net.sinchii.hadoop.tools;

import java.io.BufferedInputStream;
import java.io.DataInputStream;
import java.io.EOFException;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.fs.permission.PermissionStatus;
import org.apache.hadoop.hdfs.protocol.Block;
import org.apache.hadoop.hdfs.protocol.DatanodeID;
import org.apache.hadoop.hdfs.protocol.FSConstants;
import org.apache.hadoop.hdfs.security.token.delegation.DelegationTokenIdentifier;
import org.apache.hadoop.hdfs.server.namenode.DatanodeDescriptor;
import org.apache.hadoop.hdfs.server.namenode.FSImage;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.UTF8;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class EditsViewer extends Configured implements Tool {

	private static final byte OP_INVALID = -1;
	private static final byte OP_ADD = 0;
	private static final byte OP_CLOSE = 9;
	private static final byte OP_SET_REPLICATION = 4;
	private static final byte OP_RENAME = 1;
	private static final byte OP_DELETE = 2;
	private static final byte OP_MKDIR = 3;
	private static final byte OP_SET_GENSTAMP = 10;
	private static final byte OP_DATANODE_ADD = 5;
	private static final byte OP_DATANODE_REMOVE = 6;
	private static final byte OP_SET_PERMISSIONS = 7;
	private static final byte OP_SET_OWNER = 8;
	private static final byte OP_SET_NS_QUOTA = 11;
	private static final byte OP_CLEAR_NS_QUOTA = 12;
	private static final byte OP_TIMES = 13;
	private static final byte OP_SET_QUOTA = 14;
	private static final byte OP_GET_DELEGATION_TOKEN = 15;
	private static final byte OP_RENEW_DELEGATION_TOKEN = 16;
	private static final byte OP_CANCEL_DELEGATION_TOKEN = 17;
	private static final byte OP_UPDATE_MASTER_KEY = 18;
	
	@Override
	public int run(String[] args) throws Exception {
		
		File edits = new File(args[0]);
		DataInputStream in = new DataInputStream(new BufferedInputStream(new FileInputStream(edits)));
		
		int logVersion = 0;
		
		try {
			in.mark(4);
			boolean available = true;
			try {
				logVersion = in.readByte();
			} catch (EOFException e) {
				available = false;
			}
			
			if (available) {
				in.reset();
				logVersion = in.readInt();
				if (logVersion < FSConstants.LAYOUT_VERSION) {
					throw new IOException("Unexpected version = " + logVersion + ", Current Version = " + FSConstants.LAYOUT_VERSION);
				}
			}
			
			while (true) {
				
				// opcode
				byte opcode = -1;
				try {
					opcode = in.readByte();
					if (opcode == OP_INVALID) {
						System.err.println();
						break;
					}
				} catch (EOFException e) {
					break;
				}
				// path
				String path = null;
				// timestamp
				long timestamp = 0;
				
				switch (opcode) {
				case OP_ADD :
				case OP_CLOSE: {
					
					int length = in.readInt();
					if (-7 == logVersion && length != 3 ||
							-17 < logVersion && logVersion < -7 && length != 4 ||
							logVersion <= -17 && length != 5) {
						throw new IOException("Incorrect data format");
					}
					if (opcode == OP_ADD) {
						System.out.print("OP_ADD : ");
					} else {
						System.out.print("OP_CLOSE : ");
					}
					System.out.print("path = " + FSImage.readString(in));
					System.out.print(", replicatino = " + Short.parseShort(FSImage.readString(in)));
					System.out.print(", mtime = " + Long.parseLong(FSImage.readString(in)));
					long atime = 0;
					// atime
					if (logVersion <= -17) {
						atime = Long.parseLong(FSImage.readString(in));
					}
					System.out.print(", atime = " + atime);
					
					// block size
					long blockSize = 0;
					if (logVersion < -7) {
						blockSize = Long.parseLong(FSImage.readString(in));
					}
					System.out.print(", blockSize = " + blockSize);
					// Block
					int nB = in.readInt();
					Block b[] = new Block[nB];
					if (logVersion <= -14) {
						for (int i = 0; i < nB; i++) {
							b[i] = new Block();
							b[i].readFields(in);
							System.out.print(", Block[" + i + "]" + b[i].getBlockId());
						}
					} else {
						// Old Block
						for (int i = 0; i < nB; i++) {
							long bid = in.readLong();
							long l = in.readLong();
							b[i] = new Block(bid, l, 0);
							System.out.print(", Block[" + i + "]" + b[i].getBlockId());
						}
					}
					// Old version Block
					if (-8 <= logVersion && blockSize == 0) {
        	 if (b.length > 1) {
        		 blockSize = b[0].getNumBytes();
        	 } else {
        		 blockSize = ((b.length == 1) ? b[0].getNumBytes(): 0);
            }
          }
					// Permission
					PermissionStatus p = null;
					if (logVersion <= -11) {
						p = PermissionStatus.read(in);
						System.out.print(", owner = " + p.getUserName()
								+ ", group = " + p.getGroupName()
								+ ", permission " + p.getPermission().toShort());
					}
					if (opcode == OP_ADD && logVersion <= -12) {
						System.out.print(", clientName = " + FSImage.readString(in)
								+ ", clientMachine = " + FSImage.readString(in));
						if (-13 <= logVersion) {
							readDatanodeDescriptorArray(in);
            }
          }
					System.out.println();
					
					break;
				}
					
				case OP_SET_REPLICATION : {
					path = FSImage.readString(in);
					short replication = Short.parseShort(FSImage.readString(in));
					System.out.println("OP_SET_REPLICATION : path = " + path + ", replication = " + replication);
					break;
				}
				case OP_RENAME: {
					int length = in.readInt();
					if (length != 3) {
						throw new IOException("Incorrect data format. Rename operation");
					}
					String s = FSImage.readString(in);
					String d = FSImage.readString(in);
					timestamp = Long.parseLong(FSImage.readString(in));
					System.out.println("OP_RENAME : src = " + s + ", dst = " + d + ", timestamp = " + timestamp);
					break;
				}
				case OP_DELETE : {
					int length = in.readInt();
					if (length != 2) {
						throw new IOException("Incorrect data format. Rename operation");
					}
					path = FSImage.readString(in);
					timestamp = Long.parseLong(FSImage.readString(in));
					System.out.println("OP_DELETE : path = " + path + ", timestamp = " + timestamp);
					break;
				}
				case OP_MKDIR : {
					long atime = 0;
					PermissionStatus p = null;
					
					int length = in.readInt();
					if (-17 < logVersion && length != 2 ||
							logVersion <= -17 && length != 3) {
						throw new IOException("Incorrect data format. Rename operation");
					}
					path = FSImage.readString(in);
					timestamp = Long.parseLong(FSImage.readString(in));
					System.out.print("OP_MKDIR : path = " + path + ", timestamp = " + timestamp);					

					// atime
					if (logVersion <= -17) {
						atime = Long.parseLong(FSImage.readString(in));
						System.out.print(", atime = " + atime);
					}
					// permission
					if (logVersion <= -11) {
						 p = PermissionStatus.read(in);
						 System.out.print(", user = " + p.getUserName() + ", group = " + p.getGroupName() + p.getPermission().toString());
					}
					System.out.println();					
					break;
				}
				case OP_SET_GENSTAMP : {
					long lw = in.readLong();
					System.out.println("OP_SET_GENSTAMP : " + lw);
					break;
				}
				case OP_DATANODE_ADD : {
					DatanodeID nodeID = new DatanodeID();
					nodeID.readFields(in);
					in.readLong(); // capacity
					in.readLong(); // remaining
					in.readLong(); // lastUpdate
					in.readInt(); // xceiverCount
					break;
				}
				case OP_DATANODE_REMOVE : {
					DatanodeID nodeID = new DatanodeID();
					nodeID.readFields(in);
					break;
				}
				case OP_SET_PERMISSIONS : {
					if (logVersion > -11) {
						throw new IOException("Unexpected opcode " + opcode + " for version " + logVersion);
					}
					path = FSImage.readString(in);
					FsPermission p = FsPermission.read(in);
					System.out.println("OP_SET_PERMISSIONS : path = " + path 
							+ ", permission = " + p.toShort());
					break;
				}
				case OP_SET_OWNER : {
					if (logVersion > -11) {
						throw new IOException("Unexpected opcode " + opcode + " for version " + logVersion);
					}
					path = FSImage.readString(in);
					String user = FSImage.readString(in);
					if (user == null) {
						user = "-";
					}
					String group = FSImage.readString(in);
					if (group == null) {
						group = "-";
					}
					System.out.println("OP_SET_OWNER : path = " + path 
							+ ", user = " + user 
							+ ", group = " + group);
					break;
				}
				case OP_SET_NS_QUOTA : {
					if (logVersion > -16) {
						throw new IOException("Unexpected opcode " + opcode + " for version " + logVersion);
					}
					path = FSImage.readString(in);
					LongWritable l = new LongWritable();
					l.readFields(in);
					System.out.println("OP_SET_NS_QUOTA : path = " + path 
							+ ", quota = " + l.get());
					break;
				}
				case OP_CLEAR_NS_QUOTA : {
					if (logVersion > -16) {
						throw new IOException("Unexpected opcode " + opcode + " for version " + logVersion);
					}
					path = FSImage.readString(in);
					System.out.println("OP_CLEAR_NS_QUOTA : path = " + path);
					break;
				}
				case OP_SET_QUOTA : {
					path = FSImage.readString(in);
					LongWritable nsQuota = new LongWritable();
					LongWritable dsQuota = new LongWritable();
					System.out.println("OP_SET_QUOTA : path = " + path 
							+ ", nsQuota = " + nsQuota.get() 
							+ ", dsQuota = " + dsQuota.get());
					break;
				}
				case OP_TIMES  : {
					int length = in.readInt();
					if (length != 3) {
						throw new IOException("Incorrect data format. times operation.");
					}
					path = FSImage.readString(in);
					long mtime = Long.parseLong(FSImage.readString(in));
					long atime = Long.parseLong(FSImage.readString(in));
					System.out.println("OP_TIMES : path = " + path
							+ ", mtime = " + mtime
							+ ", atime = " + atime);
					break;
				}
				case OP_GET_DELEGATION_TOKEN : 
				case OP_RENEW_DELEGATION_TOKEN : 
				case OP_CANCEL_DELEGATION_TOKEN : 
				case OP_UPDATE_MASTER_KEY  : {
					if (logVersion > -19) {
						throw new IOException("Unexpected opcode " + opcode + " for version " + logVersion);
					}
					DelegationTokenIdentifier d = new DelegationTokenIdentifier();
					d.readFields(in);
					if (opcode != OP_CANCEL_DELEGATION_TOKEN &&
							opcode != OP_UPDATE_MASTER_KEY) {
						
						long expiryTime = Long.parseLong(FSImage.readString(in));
						if (opcode == OP_GET_DELEGATION_TOKEN) {
							System.out.print("OP_GET_DELEGATION_TOKEN : ");
						} else {
							System.out.print("OP_RENEW_DELEGATION_TOKEN : ");
						}
						System.out.println("user = " + d.getUser()
								+ ", " + d.getIssueDate()
								+ ", expirytime = " + expiryTime);
					} else if (opcode == OP_CANCEL_DELEGATION_TOKEN) {
						System.out.println("OP_CANCEL_DELEGATION_TOKEN : user = " + d.getUser());
					} else {
						System.out.println("OP_UPDATRE_MASTER_KEY : user = " + d.getUser());
					}
					break;
				}
				default : {
					throw new IOException("Never seen opcode " + opcode);
				}
				}
			}
		} catch (Throwable t) {
			//FIXME
			throw new IOException("ERROR XXX", t);			
		} finally {
			in.close();
		}

		return 0;
	}

	private void readDatanodeDescriptorArray(DataInputStream in) throws IOException {
		DatanodeDescriptor[] locations = new DatanodeDescriptor[in.readInt()];
    for (int i = 0; i < locations.length; i++) {
    	locations[i] = new DatanodeDescriptor();
    	UTF8.readString(in); // name
    	UTF8.readString(in); // storageID
    	in.readShort(); // infoPort
    	in.readLong(); // capacity
    	in.readLong(); // dfsUsed
    	in.readLong(); // remaining
    	in.readLong(); // lastUpdate
    	in.readInt(); // xceiverCount
     Text.readString(in); // location
     Text.readString(in); // hostName
     Text.readString(in); // adminState
    }
  }

	/**
	 * @param args
	 */
	public static void main(String[] args) throws Exception {
		int status = 1;
		if (args.length != 1) {
			System.err.println();
		} else {
			status = ToolRunner.run(new Configuration(), new EditsViewer(), args);
		}
		System.exit(status);
	}

}
