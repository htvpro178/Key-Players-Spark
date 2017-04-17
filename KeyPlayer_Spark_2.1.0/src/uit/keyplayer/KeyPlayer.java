package uit.keyplayer;

import java.math.BigDecimal;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.broadcast.Broadcast;

import scala.Tuple2;

public class KeyPlayer {

	public static void main(String[] args) {

		SparkConf conf = new SparkConf().setAppName("KeyPlayerSpark")
				.setMaster("local[2]");
		JavaSparkContext sc = new JavaSparkContext(conf);

		String sInputPath = "./graph_data/graph_oneline.json";
		if (args[0].equals("-in")) {
			sInputPath = args[1];

			// TODO Auto-generated method stub
			long lStart = System.currentTimeMillis();
			Graph g = new Graph();

			Data data = new Data();
			g = data.createGraphFromJSONFile(sInputPath);
			List<Vertex> vertices = g.getVertices();
			int iVertexNum = vertices.size();
			List<Edge> edges = g.getEdges();

			Utils u = new Utils(sc);

			System.out.println("" + u.GraphToString(vertices, edges));

			long lStart2 = System.currentTimeMillis();

			if (args[2].equals("-b1")) {
				lStart2 = System.currentTimeMillis();
				System.out.println("Suc anh huong cua dinh "
						+ args[3]
						+ " len dinh "
						+ args[4]
						+ " la: "
						+ u.IndirectInfluenceOfVertexOnOtherVertex(vertices,
								edges, args[3], args[4]));
			}

			if (args[2].equals("-ii")) {
				lStart2 = System.currentTimeMillis();
				System.out.println("Suc anh huong cua dinh "
						+ args[3]
						+ " toi cac dinh khac la: "
						+ u.IndirectInfluenceOfVertexOnAllVertex(vertices,
								sc.parallelize(vertices), edges, args[3]));
			}

			if (args[2].equals("-b2")) {
				lStart2 = System.currentTimeMillis();
				// Cách 1
				if (args[3].equals("c1")) {
					JavaPairRDD<String, BigDecimal> all = u
							.getAllInfluenceOfVertices(vertices, edges);
					all.cache();

					System.out.println("Suc anh huong cua tat ca cua dinh:");
					List<Tuple2<String, BigDecimal>> listAll = all.collect();
					for (Tuple2<String, BigDecimal> tuple : listAll) {
						System.out.println("[ " + tuple._1 + " : "
								+ tuple._2.toString() + " ]");
					}

					System.out.println("Key Player la: ");
					Tuple2<String, BigDecimal> kp = all.first();
					System.out.println(kp._1 + ": " + kp._2.toString());
				}
				// Cách 2
				if (args[3].equals("c2")) {
					List<Segment> listOneSegment = u.getSegmentFromEdges(
							vertices, edges);
					MongoDBSpark mongospark = new MongoDBSpark();
					mongospark.insertSegmentToMongoDB(listOneSegment);
					int iSegmentLevel = 2;
					List<Segment> listSegment = listOneSegment;
					Broadcast<List<Segment>> bcOneSegment = sc
							.broadcast(listOneSegment);
					Broadcast<List<Vertex>> bcVertices = sc.broadcast(vertices);
					while (iSegmentLevel < iVertexNum) {
						listSegment = u.getPathFromSegment(
								sc.parallelize(listSegment), bcOneSegment,
								bcVertices);
						if (listSegment.isEmpty()) {
							break;
						} else {
							mongospark.insertSegmentToMongoDB(listSegment);
							iSegmentLevel++;
						}
					}

					BigDecimal bdMax = BigDecimal.ZERO;
					String sKP = "";
					System.out.println("Suc anh huong cua tat ca cac dinh:");

					for (Vertex vertex : vertices) {
						String sVName = vertex.getName();
						BigDecimal bdTemp = u
								.getVertexIndirectInfluenceFromAllPath(mongospark
										.getVertexSegment(sVName));
						System.out.println(sVName + ": "
								+ bdTemp.toPlainString());
						if (bdTemp.compareTo(bdMax) == 1) {
							bdMax = bdTemp;
							sKP = sVName;
						}
					}

					System.out.println("Key Player la: ");
					System.out.println(sKP + ": " + bdMax.toPlainString());
				}

				if (args[3].equals("c3")) {
					// Khởi tạo ma trận kết quả
					// Map<String[], BigDecimal> mapResult = new
					// HashMap<String[], BigDecimal>();
					Map<String, Map<String, BigDecimal>> mapResult = new HashMap<String, Map<String, BigDecimal>>();

					// Khởi tạo Thread lưu ma trận kết quả
					Thread SavingThread;

					// Tính toán đoạn 1 đơn vị
					List<Segment> listOneSegment = u.getSegmentFromEdges(
							vertices, edges);

					// Lưu kết quả 1 đơn vị
					SavingThread = new MatrixResultFromSegment(mapResult,
							listOneSegment);
					SavingThread.start();

					// MongoDBSpark mongospark = new MongoDBSpark();
					// mongospark.insertSegmentToMongoDB(listOneSegment);
					int iSegmentLevel = 2;
					List<Segment> listSegment = listOneSegment;
					Broadcast<List<Segment>> bcOneSegment = sc
							.broadcast(listOneSegment);
					Broadcast<List<Vertex>> bcVertices = sc.broadcast(vertices);
					while (iSegmentLevel < iVertexNum) {
						listSegment = u.getPathFromSegment(
								sc.parallelize(listSegment), bcOneSegment,
								bcVertices);
						if (listSegment.isEmpty()) {
							break;
						} else {
							// mongospark.insertSegmentToMongoDB(listSegment);
							if (SavingThread.isAlive()) {
								try {
									SavingThread.join();
								} catch (InterruptedException e) {
									// TODO Auto-generated catch block
									e.printStackTrace();
								}
							}
							SavingThread = new MatrixResultFromSegment(
									mapResult, listSegment);
							SavingThread.start();
							iSegmentLevel++;
						}
					}

					if (SavingThread.isAlive()) {
						try {
							SavingThread.join();
						} catch (InterruptedException e) {
							// TODO Auto-generated catch block
							e.printStackTrace();
						}
					}

					/*
					 * BigDecimal bdMax = BigDecimal.ZERO; String sKP = "";
					 * System.out.println("Sức ảnh hưởng của tất cả các đỉnh:");
					 * 
					 * List<Tuple2<String, BigDecimal>> listInInf =
					 * u.getAllVertexInInfFromMatrix(sc.broadcast(mapResult),
					 * sc.parallelize(vertices));
					 * 
					 * for (Tuple2<String, BigDecimal> tuple : listInInf) {
					 * System.out.println(tuple._1 + ": " +
					 * tuple._2.toPlainString()); if (tuple._2.compareTo(bdMax)
					 * == 1){ bdMax = tuple._2; sKP = tuple._1; } }
					 * 
					 * System.out.println("Key Player là: ");
					 * System.out.println(sKP + ": " + bdMax.toPlainString());
					 */

					Entry<String, BigDecimal> KP = u.getKeyPlayerFromMatrix(
							mapResult, vertices);
					System.out.println("Key Player la: ");
					System.out.println(KP.getKey() + ": "
							+ KP.getValue().toPlainString());
				}
			}

			if (args[2].equals("-b3")) {
				lStart2 = System.currentTimeMillis();

				System.out.println("Nguong suc anh huong la: " + args[3]);
				Data.theta = new BigDecimal(args[3]);
				System.out.println("Nguong so dinh chiu anh huong la: "
						+ args[4] + "\n");
				Data.iNeed = Integer.parseInt(args[4]);
				for (int i = 0; i < vertices.size(); i++) {
					System.out.println(vertices.get(i).getName());
				}

				for (int i = 0; i < edges.size(); i++) {
					System.out.println(edges.get(i).getStartVertexName() + " "
							+ edges.get(i).getEndVertexName());
				}
				JavaPairRDD<String, List<String>> inif = u
						.getIndirectInfluence(vertices, edges);
				System.out
						.println("Suc anh huong vuot nguong cua tat ca cac dinh:");

				// In ra danh sách các đỉnh và các đỉnh chịu sức ảnh hưởng vượt
				// ngưỡng từ các đỉnh đó
				inif.foreach(f -> {
					System.out.print("\n" + f._1 + " : [");
					for (String string : f._2) {
						System.out.print(string + ", ");
					}
					System.out.print("]\n");
				});
				//

				String kp = u
						.getTheMostOverThresholdVertexName(vertices, edges);
				List<String> res = u.getSmallestGroup(vertices, edges);
				System.out
						.println("Dinh co suc anh huong vuot nguong toi cac dinh khac nhieu nhat: "
								+ kp.toString());

				System.out.println("Nhom nho nhat thoa nguong la: " + res);
			}
			// Find the smallest set which can effect to k node with influence >
			// threshold value in the network
			if (args[2].equals("-b4")) {
				lStart2 = System.currentTimeMillis();

				System.out.println("Nguong suc anh huong la: " + args[3]);
				Data.theta = new BigDecimal(args[3]);
				System.out.println("Nguong so dinh chiu anh huong la: "
						+ args[4] + "\n");
				Data.iNeed = Integer.parseInt(args[4]);
				// for (int i = 0; i <vertices.size(); i++){
				// System.out.println(vertices.get(i).getName());
				// }
				//
				// for (int i = 0; i <edges.size(); i++){
				// System.out.println(edges.get(i).getStartVertexName() + " " +
				// edges.get(i).getEndVertexName());
				// }

				List<String> resultSet = u.findSmallestGroup(vertices,
						sc.parallelize(vertices), edges);
				if (!resultSet.isEmpty()) {
					System.out.println("Tap thoa dieu kien la: " + resultSet);
				} else {
					System.out.println("Khong co tap nao thoa dieu kien");
				}

			}
			// Set of Key players
			if (args[2].equals("-b5")) {
				lStart2 = System.currentTimeMillis();

				System.out
						.println("So dinh Key players can tim la: " + args[3]);
				Data.iNeed = Integer.parseInt(args[3]);

				JavaPairRDD<List<String>, BigDecimal> all = u
						.findSetKeyPlayers(vertices, edges);
				all.cache();

				System.out.println("Suc anh huong cua tat ca cua dinh:");
				List<Tuple2<List<String>, BigDecimal>> listAll = all.collect();
				for (Tuple2<List<String>, BigDecimal> tuple : listAll) {
					System.out.println("[ " + tuple._1 + " : "
							+ tuple._2.toString() + " ]");
				}

				System.out.println("Tap Key Players: ");
				Tuple2<List<String>, BigDecimal> kp = all.first();
				System.out.println(kp._1 + ": " + kp._2.toString());
			}

			long lEnd = System.currentTimeMillis();

			System.out.println("Thoi gian tong cong la: " + (lEnd - lStart)
					+ " ms");

			System.out
					.println("Thoi gian tinh toan khong tinh thoi gian tao do thi: "
							+ (lEnd - lStart2) + " ms");
		}
	}
}
