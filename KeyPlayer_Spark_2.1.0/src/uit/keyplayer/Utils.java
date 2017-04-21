package uit.keyplayer;

import java.io.Serializable;
import java.math.BigDecimal;
import java.math.RoundingMode;
import java.util.AbstractMap;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.broadcast.Broadcast;
//import org.apache.spark.sql.types.Decimal;

import org.omg.CORBA.DATA_CONVERSION;

import scala.Array;
import scala.Tuple2;

public class Utils implements Serializable {
	public static boolean ASC = true;
	public static boolean DESC = false;
	// private Broadcast<JavaRDD<Vertex>> bcVertices;
	// private Broadcast<JavaRDD<Edge>> bcEdges;
	// private List<Vertex> vertices;
	// private List<Edge> edges;
	private JavaPairRDD<String, List<String>> indirectInfluence;
	private JavaPairRDD<String, Map<String, BigDecimal>> mapResult1;
	private List<Tuple2<String, Integer>> indirectInfluenceCount;
	// private JavaPairRDD<String, List<String>> influence;
	private Map<String, Map<String, BigDecimal>> mapResult;
	private Map<String, List<String>> mapInfluented;
	private Map<String, Map<String, BigDecimal>> mapResultFirst;
	private Map<String, Map<String, List<List<String>>>> mapInfluencePath;
	private Map<String, List<String>> mapInfluentedFirst;
	private ArrayList<String> result;
	private long lCount; // dem so luong to hop phai duyet qua, old b3, not use
	// private long lCountChecking; // dem so luong to hop phai duyet qua, old
	// b3, not use
	private long m_countChecking;
	private List<String> keyPlayerSet;
	private transient JavaSparkContext sc;

	public Utils(JavaSparkContext sc) {
		this.indirectInfluence = null;
		this.indirectInfluenceCount = null;
		// this.influence = null;
		this.result = null;
		this.lCount = 0;
		// this.vertices = vertices;
		// this.edges = edges;
		this.mapResult = new HashMap<String, Map<String, BigDecimal>>();
		this.mapInfluented = new HashMap<String, List<String>>();
		this.mapResultFirst = new HashMap<String, Map<String, BigDecimal>>();
		this.mapInfluencePath = new HashMap<String, Map<String, List<List<String>>>>();
		this.mapInfluentedFirst = new HashMap<String, List<String>>();
		this.m_countChecking = 0;
		this.keyPlayerSet = new ArrayList<>();
		this.sc = sc;
	}

	public Vertex getVertexFromName(List<Vertex> vertices, String sName) {
		// List<Vertex> vertices = bcVertices.value().collect();
		for (Vertex vertex : vertices) {
			if (vertex.getName().equals(sName)) {
				return vertex;
			}
		}
		return null;
	}

	public BigDecimal getVertexSpreadCoefficientFromName(List<Vertex> vertices,
			String sName, String sStartName) {
		Vertex vertex = getVertexFromName(vertices, sName);
		if (vertex != null) {
			// if
			// (vertex.getSpreadCoefficientFromVertexName(sStartName).compareTo(
			// new BigDecimal("1")) != 0){
			// return vertex.getSpreadCoefficientFromVertexName(sStartName);
			// }
			// else {
			return BigDecimal.ONE;
			// }
		}
		return new BigDecimal("-1");
	}

	public Edge getEdgeFromStartEndVertex(List<Edge> edges, String sStart,
			String sEnd) {
		for (Edge edge : edges) {
			if (edge.getStartVertexName().equals(sStart)
					&& edge.getEndVertexName().equals(sEnd)) {
				return edge;
			}
		}
		return null;
	}

	public BigDecimal getEdgeDirectInfluenceFromStartEndVertex(
			List<Edge> edges, String sStart, String sEnd) {
		Edge edge = getEdgeFromStartEndVertex(edges, sStart, sEnd);
		if (edge != null) {
			return edge.getDirectInfluence();
		}
		return new BigDecimal("-1");
	}

	public List<Edge> getEdgesStartAtVertex(List<Edge> edges,
			String sStartVertexName) {
		List<Edge> listEdge = new ArrayList<Edge>();

		for (Edge edge : edges) {
			if (edge.getStartVertexName().equals(sStartVertexName)) {
				listEdge.add(edge);
			}
		}

		return listEdge;
	}

	public List<Edge> getEdgesEndAtVertex(List<Edge> edges,
			String sEndVertexName) {
		List<Edge> listEdge = new ArrayList<Edge>();

		for (Edge edge : edges) {
			if (edge.getEndVertexName().equals(sEndVertexName)) {
				listEdge.add(edge);
			}
		}

		return listEdge;
	}

	public List<String> getVerticesPointedByVertex(List<Edge> edges,
			String sVertexName) {
		List<String> result = new ArrayList<String>();
		List<Edge> listEdges = getEdgesStartAtVertex(edges, sVertexName);

		for (Edge edge : listEdges) {
			result.add(edge.getEndVertexName());
		}

		return result;
	}

	public List<List<String>> getAllPathBetweenTwoVertex(List<Edge> edges,
			String sStart, String sEnd) {
		List<List<String>> result = new ArrayList<List<String>>(); // ket qua
																	// tra ve la
																	// tat ca
																	// path
		List<String> temp = new ArrayList<String>();
		List<String> explored = new ArrayList<String>(); // danh dau nhung dinh
															// da tham

		List<String> endpath = new ArrayList<String>();
		endpath.add(sEnd);
		boolean fChangeEnd = false;
		List<Edge> whetherOneEdge = getEdgesEndAtVertex(edges, sEnd);
		while (whetherOneEdge.size() <= 1) {
			if (whetherOneEdge.size() == 0) {
				return null;
			} else {
				String sStartName = whetherOneEdge.get(0).getStartVertexName();
				fChangeEnd = true;
				endpath.add(0, sStartName);
				if (sStartName.equals(sStart)) {
					return Arrays.asList(endpath);
				} else {
					sEnd = sStartName;
					whetherOneEdge = getEdgesEndAtVertex(edges, sEnd);
				}
			}
		}
		if (fChangeEnd) {
			endpath.remove(0);
		} else {
			endpath.clear();
		}

		temp.add(sStart); // tham S
		explored.add(sStart); // danh dau S da tham
		List<String> listCandidate = getVerticesPointedByVertex(edges, sStart);
		List<Integer> iCandidate = new ArrayList<Integer>();
		if (listCandidate != null) {
			iCandidate.add(listCandidate.size());

			while (!listCandidate.isEmpty()) {
				int iCount = listCandidate.size();
				String sCandidate = listCandidate.remove(iCount - 1);
				int iLast = iCandidate.size() - 1;
				iCandidate.set(iLast, iCandidate.get(iLast) - 1);
				if (!explored.contains(sCandidate)) {
					temp.add(sCandidate);
					explored.add(sCandidate);
					if (sCandidate.equals(sEnd)) {
						// List<String> onePath = new
						// ArrayList<String>(temp.size());
						// cloneStringList(temp, onePath);
						// onePath =
						// (List<String>)(((ArrayList<String>)temp).clone());
						if (fChangeEnd) {
							temp.addAll(endpath);
							result.add((List<String>) (((ArrayList<String>) temp)
									.clone()));
							temp.removeAll(endpath);
						} else {
							result.add((List<String>) (((ArrayList<String>) temp)
									.clone()));
						}
						explored.remove(sCandidate);
						temp.remove(sCandidate);
						while (!iCandidate.isEmpty()
								&& iCandidate.get(iCandidate.size() - 1) == 0) {
							temp.remove(temp.size() - 1);
							explored.remove(explored.size() - 1);
							iCandidate.remove(iCandidate.size() - 1);
						}
					} else {
						List<String> listNewCandidate = getVerticesPointedByVertex(
								edges, sCandidate);
						if (listNewCandidate != null
								&& !listNewCandidate.isEmpty()) {
							listCandidate.addAll(listNewCandidate);
							iCandidate.add(listNewCandidate.size());
						} else {
							temp.remove(sCandidate);
							explored.remove(sCandidate);
							while (iCandidate.get(iCandidate.size() - 1) == 0) {
								temp.remove(temp.size() - 1);
								explored.remove(explored.size() - 1);
								iCandidate.remove(iCandidate.size() - 1);
								if (iCandidate.isEmpty()) {
									break;
								}
							}
						}
					}
				} else {
					while (!iCandidate.isEmpty()
							&& iCandidate.get(iCandidate.size() - 1) == 0) {
						temp.remove(temp.size() - 1);
						explored.remove(explored.size() - 1);
						iCandidate.remove(iCandidate.size() - 1);
					}
				}
			}
		}

		return ((result != null) && (!result.isEmpty())) ? result : null;
	}

	public List<List<String>> IndirectInfluenceOfVertexOnOtherVertex(
			List<Vertex> vertices, List<Edge> edges, String sStartName,
			String sEndName) {
		BigDecimal fIndirectInfluence = BigDecimal.ONE;

		/*
		 * final Broadcast<List<Vertex>> bcVertices = sc.broadcast(vertices);
		 * final Broadcast<List<Edge>> bcEdges = sc.broadcast(edges);
		 * 
		 * List<List<String>> listPath =
		 * getAllPathBetweenTwoVertex(bcEdges.value(), sStartName, sEndName); if
		 * (listPath != null) { JavaRDD<List<String>> rddAllPath =
		 * sc.parallelize(listPath); rddAllPath.cache();
		 * 
		 * if (rddAllPath != null) { fIndirectInfluence = rddAllPath.map(path ->
		 * { BigDecimal bdTemp = BigDecimal.ZERO; String sBefore = null; for
		 * (String v : path) { if (sBefore != null) { bdTemp =
		 * bdTemp.add(getVertexSpreadCoefficientFromName(bcVertices.value(), v,
		 * sBefore)
		 * .multiply(getEdgeDirectInfluenceFromStartEndVertex(bcEdges.value(),
		 * sBefore, v))); if (bdTemp.compareTo(BigDecimal.ONE) == 1) { return
		 * BigDecimal.ONE; } } sBefore = v; } return bdTemp; }).reduce((bd1,
		 * bd2) -> bd1.add(bd2)); } }
		 */

		List<String> temp = new ArrayList<String>();
		List<String> explored = new ArrayList<String>(); // danh dau nhung dinh
															// da them
		List<List<String>> listStringPath = new ArrayList<List<String>>();

		List<String> endpath = new ArrayList<String>();
		endpath.add(sEndName);
		boolean fChangeEnd = false;
		List<Edge> whetherOneEdge = getEdgesEndAtVertex(edges, sEndName);
		while (whetherOneEdge.size() <= 1) {
			if (whetherOneEdge.size() == 0) {
				// System.out.println("Khong co duong di tu dinh " + sStartName
				// + " den dinh " + sEndName);
				List<String> tempValue = new ArrayList<String>();
				tempValue.add(String.valueOf(BigDecimal.ZERO));
				listStringPath.add(0, tempValue);
				return listStringPath;
			} else {
				String sStart = whetherOneEdge.get(0).getStartVertexName();
				endpath.add(0, sStart);
				if (sStart.equals(sStartName)) {
					String sBefore = null;
					for (String v : endpath) {
						if (sBefore != null) {
							fIndirectInfluence = fIndirectInfluence
									.multiply(getVertexSpreadCoefficientFromName(
											vertices, v, sBefore)
											.multiply(
													getEdgeDirectInfluenceFromStartEndVertex(
															edges, sBefore, v)));
							if (fIndirectInfluence.compareTo(BigDecimal.ONE) != -1) {
//								System.out
//										.println("Duong di duy nhat vua duoc tinh truoc khi ngat: "
//												+ endpath);
								System.out.println(endpath);
								List<String> tempValue = new ArrayList<String>();
								tempValue.add(String.valueOf(BigDecimal.ONE));
								listStringPath.add(0, tempValue);
								listStringPath.add(endpath);
								return listStringPath;
							}
						}
						sBefore = v;
					}
//					System.out.println("Duong di duy nhat vua duoc tinh la: "
//							+ endpath);
					System.out.println(endpath);
					List<String> tempValue = new ArrayList<String>();
					tempValue.add(String.valueOf(fIndirectInfluence));
					listStringPath.add(tempValue);
					listStringPath.add(endpath);
					return listStringPath;
				} else {
					fChangeEnd = true;
					sEndName = sStart;
					whetherOneEdge = getEdgesEndAtVertex(edges, sEndName);
				}
			}
		}
		if (fChangeEnd) {
			endpath.remove(0);
		} else {
			endpath.clear();
		}

		temp.add(sStartName); // tham S
		explored.add(sStartName); // danh dau S da tham
		List<String> listCandidate = getVerticesPointedByVertex(edges,
				sStartName);
		List<Integer> iCandidate = new ArrayList<Integer>();
		if (listCandidate != null) {
			iCandidate.add(listCandidate.size());

			while (!listCandidate.isEmpty()) {
				int iCount = listCandidate.size();
				String sCandidate = listCandidate.remove(iCount - 1);
				int iLast = iCandidate.size() - 1;
				iCandidate.set(iLast, iCandidate.get(iLast) - 1);
				if (!explored.contains(sCandidate)) {
					temp.add(sCandidate);
					explored.add(sCandidate);
					if (sCandidate.equals(sEndName)) {
						if (fChangeEnd) {
							temp.addAll(endpath);
						}

						String sBefore = null;
						BigDecimal bdPartial = BigDecimal.ONE;
						for (String v : temp) {
							if (sBefore != null) {
								bdPartial = bdPartial
										.multiply(getVertexSpreadCoefficientFromName(
												vertices, v, sBefore)
												.multiply(
														getEdgeDirectInfluenceFromStartEndVertex(
																edges, sBefore,
																v)));
								/*
								 * if
								 * (fIndirectInfluence.compareTo(BigDecimal.ONE)
								 * != -1) { System.out.println(
								 * "Đường đi vừa được tính trước khi ngắt là: "
								 * + temp); return BigDecimal.ONE; }
								 */
							}
							sBefore = v;
						}
						fIndirectInfluence = fIndirectInfluence
								.multiply(BigDecimal.ONE.subtract(bdPartial));
//						System.out
//								.println("Duong di vua duoc tinh la: " + temp);
						System.out.println(temp);
						List<String> temp1 = new ArrayList<String>();
						for (String t : temp) {
							// System.out.println("Temp=" + t);
							temp1.add(t);
						}
						listStringPath.add(temp1);
						// System.out.println("listStringPath=" +
						// listStringPath);

						if (fChangeEnd) {
							temp.removeAll(endpath);
						}

						explored.remove(sCandidate);
						temp.remove(sCandidate);
						while (!iCandidate.isEmpty()
								&& iCandidate.get(iCandidate.size() - 1) == 0) {
							temp.remove(temp.size() - 1);
							explored.remove(explored.size() - 1);
							iCandidate.remove(iCandidate.size() - 1);
						}
					} else {
						List<String> listNewCandidate = getVerticesPointedByVertex(
								edges, sCandidate);
						if (listNewCandidate != null
								&& !listNewCandidate.isEmpty()) {
							listCandidate.addAll(listNewCandidate);
							iCandidate.add(listNewCandidate.size());
						} else {
							temp.remove(sCandidate);
							explored.remove(sCandidate);
							while (iCandidate.get(iCandidate.size() - 1) == 0) {
								temp.remove(temp.size() - 1);
								explored.remove(explored.size() - 1);
								iCandidate.remove(iCandidate.size() - 1);
								if (iCandidate.isEmpty()) {
									break;
								}
							}
						}
					}
				} else {
					while (!iCandidate.isEmpty()
							&& iCandidate.get(iCandidate.size() - 1) == 0) {
						temp.remove(temp.size() - 1);
						explored.remove(explored.size() - 1);
						iCandidate.remove(iCandidate.size() - 1);
					}
				}
			}

		}
		// return (fIndirectInfluence.compareTo(BigDecimal.ONE) == 1) ?
		// BigDecimal.ONE : fIndirectInfluence;
		List<String> tempValue = new ArrayList<String>();
		tempValue.add(String.valueOf(BigDecimal.ONE
				.subtract(fIndirectInfluence)));
		listStringPath.add(0, tempValue);
		// System.out.println("listPa DAY" + listPa);
		// System.out.println("DAY");
		// System.out.println("A=" + a);
		return listStringPath;
	}

	public BigDecimal InfluenceOfSetVertexOnOtherVertex(List<Vertex> vertices,
			List<Edge> edges, List<String> sSetVertex, String sStartName,
			String sEndName) {
		BigDecimal fIndirectInfluence = BigDecimal.ONE;

		List<String> temp = new ArrayList<String>();
		List<String> explored = new ArrayList<String>(); // danh dau nhung dinh
															// da tham

		List<String> endpath = new ArrayList<String>();
		endpath.add(sEndName);
		boolean fChangeEnd = false;
		List<Edge> whetherOneEdge = getEdgesEndAtVertex(edges, sEndName);
		while (whetherOneEdge.size() <= 1) {
			if (whetherOneEdge.size() == 0 || sSetVertex.contains(sEndName)) {
				// System.out.println("Khong co duong di tu dinh " + sStartName
				// + " den dinh " + sEndName);
				return BigDecimal.ZERO;
			} else {
				String sStart = whetherOneEdge.get(0).getStartVertexName();
				endpath.add(0, sStart);
				if (sStart.equals(sStartName)) {
					String sBefore = null;
					for (String v : endpath) {
						// Dinh sBefore khong nam trong bo S
						if (sBefore != null) {
							if (!sSetVertex.contains(sBefore)
									|| sBefore.equals(sStartName)) {
								fIndirectInfluence = fIndirectInfluence
										.multiply(getVertexSpreadCoefficientFromName(
												vertices, v, sBefore)
												.multiply(
														getEdgeDirectInfluenceFromStartEndVertex(
																edges, sBefore,
																v)));
								if (fIndirectInfluence
										.compareTo(BigDecimal.ONE) != -1) {
									// System.out
									// .println("Duong di duy nhat vua duoc tinh truoc khi ngat: "
									// + endpath);
									System.out.println(endpath);
									return BigDecimal.ONE;
								}
							} else {
								// System.out.println("Khong co duong di tu dinh "
								// + sStartName + " den dinh " + sEndName);
								return BigDecimal.ZERO;
							}
						}
						sBefore = v;
					}
					// System.out.println("Duong di duy nhat vua duoc tinh la: "
					// + endpath);
					System.out.println(endpath);
					return fIndirectInfluence;
				} else {
					fChangeEnd = true;
					sEndName = sStart;
					whetherOneEdge = getEdgesEndAtVertex(edges, sEndName);
				}
			}
		}
		if (fChangeEnd) {
			endpath.remove(0);
		} else {
			endpath.clear();
		}

		temp.add(sStartName); // tham S
		explored.add(sStartName); // danh dau S da tham
		List<String> listCandidate = getVerticesPointedByVertex(edges,
				sStartName);
		List<Integer> iCandidate = new ArrayList<Integer>();
		if (listCandidate != null) {
			iCandidate.add(listCandidate.size());

			while (!listCandidate.isEmpty()) {
				int iCount = listCandidate.size();
				String sCandidate = listCandidate.remove(iCount - 1);
				int iLast = iCandidate.size() - 1;
				iCandidate.set(iLast, iCandidate.get(iLast) - 1);
				if (!explored.contains(sCandidate)
						&& (!sSetVertex.contains(sCandidate))) {
					temp.add(sCandidate);
					explored.add(sCandidate);
					if (sCandidate.equals(sEndName)) {
						if (fChangeEnd) {
							temp.addAll(endpath);
						}

						String sBefore = null;
						BigDecimal bdPartial = BigDecimal.ONE;
						for (String v : temp) {
							if (sBefore != null) {
								bdPartial = bdPartial
										.multiply(getVertexSpreadCoefficientFromName(
												vertices, v, sBefore)
												.multiply(
														getEdgeDirectInfluenceFromStartEndVertex(
																edges, sBefore,
																v)));
								/*
								 * if
								 * (fIndirectInfluence.compareTo(BigDecimal.ONE)
								 * != -1) { System.out.println(
								 * "Đường đi vừa được tính trước khi ngắt là: "
								 * + temp); return BigDecimal.ONE; }
								 */
							}
							sBefore = v;
						}
						fIndirectInfluence = fIndirectInfluence
								.multiply(BigDecimal.ONE.subtract(bdPartial));
						// System.out
						// .println("Duong di vua duoc tinh la: " + temp);
						System.out.println(temp);

						if (fChangeEnd) {
							temp.removeAll(endpath);
						}

						explored.remove(sCandidate);
						temp.remove(sCandidate);
						while (!iCandidate.isEmpty()
								&& iCandidate.get(iCandidate.size() - 1) == 0) {
							temp.remove(temp.size() - 1);
							explored.remove(explored.size() - 1);
							iCandidate.remove(iCandidate.size() - 1);
						}
					} else {
						List<String> listNewCandidate = getVerticesPointedByVertex(
								edges, sCandidate);
						if (listNewCandidate != null
								&& !listNewCandidate.isEmpty()) {
							listCandidate.addAll(listNewCandidate);
							iCandidate.add(listNewCandidate.size());
						} else {
							temp.remove(sCandidate);
							explored.remove(sCandidate);
							while (iCandidate.get(iCandidate.size() - 1) == 0) {
								temp.remove(temp.size() - 1);
								explored.remove(explored.size() - 1);
								iCandidate.remove(iCandidate.size() - 1);
								if (iCandidate.isEmpty()) {
									break;
								}
							}
						}
					}
				} else {
					while (!iCandidate.isEmpty()
							&& iCandidate.get(iCandidate.size() - 1) == 0) {
						temp.remove(temp.size() - 1);
						explored.remove(explored.size() - 1);
						iCandidate.remove(iCandidate.size() - 1);
					}
				}
			}
		}
		return BigDecimal.ONE.subtract(fIndirectInfluence);
	}

	public BigDecimal IndirectInfluenceOfVertexOnAllVertex(
			List<Vertex> vertices, JavaRDD<Vertex> rddVertices,
			List<Edge> edges, String sVertexName) {
		BigDecimal fIndirectInfluence = BigDecimal.ZERO;
//		System.out.println("Xet dinh " + sVertexName);
		final Broadcast<List<Vertex>> bcVertices = sc.broadcast(vertices);
		final Broadcast<List<Edge>> bcEdges = sc.broadcast(edges);
		final Broadcast<String> bcVertexName = sc.broadcast(sVertexName);

		// rddVertices.cache();
		JavaPairRDD<List<List<String>>, BigDecimal> rddIndrInfl = rddVertices
				.mapToPair(vertex -> {
					String vName = vertex.getName();
					if (!vName.equals(bcVertexName.value())) {
						List<List<String>> listResult = IndirectInfluenceOfVertexOnOtherVertex(
								bcVertices.value(), bcEdges.value(),
								bcVertexName.value(), vName);
						BigDecimal bd = new BigDecimal(listResult.get(0).get(0));
						// System.out.println("bd=" + bd);
						List<List<String>> listResultAfterCut = new ArrayList<List<String>>();
						for (int ii = 1; ii < listResult.size(); ii++) {
							// System.out.println("listResult.get(" + ii +
							// ")= "+ listResult.get(ii));
							listResultAfterCut.add(listResult.get(ii));
						}
						return new Tuple2<List<List<String>>, BigDecimal>(
								listResultAfterCut, bd);
					} else {
						// System.out
						// .println("Suc anh huong gian tiep den chinh dinh do la 0.");
						List<List<String>> listResultAfterCut = new ArrayList<List<String>>();
						return new Tuple2<List<List<String>>, BigDecimal>(
								listResultAfterCut, BigDecimal.ZERO);
					}
				});
		rddIndrInfl.cache();

		// rddIndrInfl.foreach(data -> {
		// System.out.println("model="+ data._1() + " label=" + data._2());
		// for ( List<String> list : data._1()){
		// System.out.println("list=" + list);
		// }
		// });

		fIndirectInfluence = rddIndrInfl.values().reduce(
				(bd1, bd2) -> bd1.add(bd2));
		// final Broadcast<BigDecimal> bcTheta = sc.broadcast(Data.theta);
		Map<String, List<List<String>>> mapPathWithEnd = new HashMap<String, List<List<String>>>();
		Map<String, BigDecimal> infValueAfterCal = new HashMap<String, BigDecimal>();
		Map<List<List<String>>, BigDecimal> map = rddIndrInfl.collectAsMap();
		HashMap<List<List<String>>, BigDecimal> hmap = new HashMap<List<List<String>>, BigDecimal>(
				map);
		List<String> sInfluentedVertex = new ArrayList<>();
		for (Entry<List<List<String>>, BigDecimal> entry : hmap.entrySet()) {
			if (entry.getValue().compareTo(BigDecimal.ZERO) == 1) {
//				System.out.println("Key : " + entry.getKey() + " Value : "
//						+ entry.getValue());
				// System.out.println("si=" +
				// entry.getKey().get(0).get(entry.getKey().get(0).size() - 1
				// ));
				String sEnd = entry.getKey().get(0)
						.get(entry.getKey().get(0).size() - 1);
				mapPathWithEnd.put(sEnd, entry.getKey());
				infValueAfterCal.put(sEnd, entry.getValue());
				sInfluentedVertex.add(sEnd);
			}
		}
		this.mapInfluencePath.put(sVertexName, mapPathWithEnd); // this map
																// includes
																// start, end,
																// all paths
																// from start to
																// end
		this.mapResult.put(sVertexName, infValueAfterCal); // this map includes
															// influence from
															// start to end
		this.mapInfluented.put(sVertexName, sInfluentedVertex); // this map
																// includes
																// influented
																// vertexes of
																// start vertex
		// if (indirectInfluence != null) {
		// // if (indirectInfluence.lookup(sVertexName).isEmpty()) {
		// indirectInfluence = indirectInfluence.union(OverThresholdVertex);
		// // }
		// } else {
		// indirectInfluence = OverThresholdVertex;
		// indirectInfluence.cache();
		// }
		//

		// Map<String, BigDecimal> influentedValue = new HashMap<String,
		// BigDecimal>();
		//
		// for (Vertex vertex : vertices) {
		// BigDecimal influented = new
		// BigDecimal(String.valueOf(infValueAfterCal.get(vertex.getName())));
		// if (influented.compareTo(BigDecimal.ZERO) == 1) {
		// sInfluentedVertex.add(vertex.getName());
		// }
		// influentedValue.put(vertex.getName(), influented);
		// System.out.println("influented " + influented);
		//
		// }
		// this.mapInfluented.put(sVertexName, sInfluentedVertex);

		// System.out.println("fIndirectInfluence=" + fIndirectInfluence);
		return fIndirectInfluence;
	}

	public BigDecimal InfluenceOfSetVertexOnAllVertex(List<Vertex> vertices,
			JavaRDD<Vertex> rddVertices, List<Edge> edges,
			List<String> sSetVertexName, String sVertexName) {
		BigDecimal fInfluence = BigDecimal.ZERO;

		final Broadcast<List<Vertex>> bcVertices = sc.broadcast(vertices);
		final Broadcast<List<Edge>> bcEdges = sc.broadcast(edges);
		final Broadcast<String> bcVertexName = sc.broadcast(sVertexName);

		// rddVertices.cache();

		JavaPairRDD<String, BigDecimal> rddIndrInfl = rddVertices
				.mapToPair(vertex -> {
					String vName = vertex.getName();
					if (!vName.equals(bcVertexName.value())) {
						BigDecimal bd = InfluenceOfSetVertexOnOtherVertex(
								bcVertices.value(), bcEdges.value(),
								sSetVertexName, bcVertexName.value(), vName);
						return new Tuple2<String, BigDecimal>(vName, bd);
					} else {
						// System.out
						// .println("Suc anh huong gian tiep den chinh dinh do la 0.");
				return new Tuple2<String, BigDecimal>(vName, BigDecimal.ZERO);
			}
		});

		rddIndrInfl.cache();
		fInfluence = rddIndrInfl.values().reduce((bd1, bd2) -> bd1.add(bd2));

		Map<String, BigDecimal> influentedValue = new HashMap<String, BigDecimal>();
		List<String> sInfluentedVertex = new ArrayList<>();
		for (Vertex vertex : vertices) {
			BigDecimal influented = new BigDecimal(String.valueOf(rddIndrInfl
					.lookup(vertex.getName()).get(0)));
			if (influented.compareTo(BigDecimal.ZERO) == 1) {
				sInfluentedVertex.add(vertex.getName());
			}
			influentedValue.put(vertex.getName(), influented);
			// System.out.println("influented " + influented);
			this.mapResult.put(sVertexName, influentedValue);
			// if the Set S include 1 vertex
			if (sSetVertexName.size() == 1) {
				this.mapResultFirst.put(sVertexName, influentedValue);
			}
		}
		this.mapInfluented.put(sVertexName, sInfluentedVertex);
		// if the Set S include 1 vertex
		if (sSetVertexName.size() == 1) {
			this.mapInfluentedFirst.put(sVertexName, sInfluentedVertex);
		}
		return fInfluence;
	}

	// public BigDecimal InfluenceOfSetCandidateOnAllVertex(List<Vertex>
	// vertices,
	// JavaRDD<Vertex> rddVertices, List<Edge> edges,
	// List<String> sSetVertexName, String sVertexName) {
	// BigDecimal fIndirectInfluence = BigDecimal.ZERO;
	// final Broadcast<List<Vertex>> bcVertices = sc.broadcast(vertices);
	// final Broadcast<List<Edge>> bcEdges = sc.broadcast(edges);
	// final Broadcast<String> bcVertexName = sc.broadcast(sVertexName);
	//
	// // rddVertices.cache();
	//
	// JavaPairRDD<String, BigDecimal> rddIndrInfl = rddVertices
	// .mapToPair(vertex -> {
	// String vName = vertex.getName();
	// if (!vName.equals(bcVertexName.value())) {
	// BigDecimal bd = IndirectInfluenceOfVertexOnOtherVertex(
	// bcVertices.value(), bcEdges.value(),
	// bcVertexName.value(), vName);
	// return new Tuple2<String, BigDecimal>(vName, bd);
	// } else {
	// System.out
	// .println("Suc anh huong gian tiep den chinh dinh do la 0.");
	// return new Tuple2<String, BigDecimal>(vName,
	// BigDecimal.ZERO);
	// }
	// });
	// rddIndrInfl.cache();
	//
	// fIndirectInfluence = rddIndrInfl.values().reduce(
	// (bd1, bd2) -> bd1.add(bd2));
	//
	// final Broadcast<BigDecimal> bcTheta = sc.broadcast(Data.theta);
	//
	// /*
	// * JavaPairRDD<String,List<String>> OverThresholdVertex =
	// * rddIndrInfl.filter(tuple -> { return
	// * (tuple._2.compareTo(bcTheta.value()) != -1); }).mapToPair(pairSB ->{
	// * return new Tuple2<String, List<String>>(bcVertexName.value(), new
	// * ArrayList<String>(Arrays.asList(pairSB._1))); }).reduceByKey((l1, l2)
	// * -> { l1.addAll(l2); return l1; });
	// */
	//
	// JavaPairRDD<String, List<String>> OverThresholdVertex = sc
	// .parallelizePairs(Arrays
	// .asList(new Tuple2<String, List<String>>(sVertexName,
	// rddIndrInfl
	// .filter(tuple -> {
	// return (tuple._2.compareTo(bcTheta
	// .value()) != -1);
	// }).keys().collect())));
	// if (indirectInfluence != null) {
	// // if (indirectInfluence.lookup(sVertexName).isEmpty()) {
	// indirectInfluence = indirectInfluence.union(OverThresholdVertex);
	// // }
	// } else {
	// indirectInfluence = OverThresholdVertex;
	// indirectInfluence.cache();
	// }
	// return fIndirectInfluence;
	// }

	public JavaPairRDD<String, BigDecimal> getAllInfluenceOfVertices(
			List<Vertex> vertices, List<Edge> edges) {
		List<Tuple2<BigDecimal, String>> mUnsortedAll = new ArrayList<Tuple2<BigDecimal, String>>(
				vertices.size());
		// List<Vertex> vertices = bcVertices.value().collect();
		JavaRDD<Vertex> rddVertices = sc.parallelize(vertices);
		rddVertices.cache();

		for (Vertex vertex : vertices) {
			String vName = vertex.getName();
			mUnsortedAll.add(new Tuple2<BigDecimal, String>(
					IndirectInfluenceOfVertexOnAllVertex(vertices, rddVertices,
							edges, vName), vName));
		}

		return sc.parallelizePairs(mUnsortedAll).sortByKey(false)
				.mapToPair(t -> t.swap());
	}

	public JavaPairRDD<String, BigDecimal> getAllInfluenceOfSetVertices(
			List<Vertex> vertices, List<Edge> edges, List<String> keyPlayerSetIn) {
		List<Tuple2<BigDecimal, String>> mUnsortedAll = new ArrayList<Tuple2<BigDecimal, String>>(
				vertices.size());
		// List<Vertex> vertices = bcVertices.value().collect();
		JavaRDD<Vertex> rddVertices = sc.parallelize(vertices);
		rddVertices.cache();

		for (Vertex vertex : vertices) {
			if (!stringContainsItemFromList(vertex.getName(), keyPlayerSetIn)) {
				String vName = vertex.getName();
				List<String> keyCandidates = new ArrayList<String>();
				for (String key : keyPlayerSetIn) {
					keyCandidates.add(key);
				}
				keyCandidates.add(vName);
				System.out.println("\n***Xet tap: " + keyCandidates);
				mUnsortedAll.add(new Tuple2<BigDecimal, String>(
						CalculateInfluenceOfSetCandidates(vertices, edges,
								keyCandidates), vName));
			}
		}
		return sc.parallelizePairs(mUnsortedAll).sortByKey(false)
				.mapToPair(t -> t.swap());
	}

	private BigDecimal calculateInfluenceByPath(List<Vertex> vertices,
			List<Edge> edges, List<String> path) {
		String sBefore = null;
		BigDecimal infValue = BigDecimal.ONE;
		for (String v : path) {
			if (sBefore != null) {
				infValue = infValue
						.multiply(getVertexSpreadCoefficientFromName(vertices,
								v, sBefore).multiply(
								getEdgeDirectInfluenceFromStartEndVertex(edges,
										sBefore, v)));
			}
			sBefore = v;
		}
		return infValue;
	}

	private BigDecimal CalculateInfluenceOfSetCandidates(List<Vertex> vertices,
			List<Edge> edges, List<String> keyCandidates) {
		BigDecimal influenceOfSetVertices = BigDecimal.ZERO;
		Map<String, Map<String, BigDecimal>> influenceValue = new HashMap<String, Map<String, BigDecimal>>();
		Map<String, List<String>> influentVertices = new HashMap<String, List<String>>();
		Map<String, BigDecimal> inf = new HashMap<String, BigDecimal>();
		// System.out.println("keyCandidates=" + keyCandidates);
		for (String keyCan : keyCandidates) {
			List<String> listKeyCan = new ArrayList<String>();
			for (String st : keyCandidates) {
				listKeyCan.add(st);
			}
			listKeyCan.remove(keyCan); // tap S \ dinh dang xet suc anh huong
			BigDecimal infValue = BigDecimal.ZERO;
			List<String> influentVertex = new ArrayList<String>();
			// System.out.println("this.mapInfluented.get(keyCan)=" +
			// this.mapInfluented.get(keyCan));
			for (String vertex : this.mapInfluented.get(keyCan)) {
				// System.out.println("keyCan=" + keyCan);
				// System.out.println("vertex=" + vertex);
				infValue = BigDecimal.ONE.subtract(this.mapResult.get(keyCan)
						.get(vertex));
				// System.out.println("infValue=" + infValue);
				if (this.mapInfluencePath != null) {
					int countRemovePath = 0;
					for (List<String> listStr : this.mapInfluencePath.get(
							keyCan).get(vertex)) {
						// System.out.println("listStr: " + listStr);
						if (!Collections.disjoint(listKeyCan, listStr)) {
							// Duong di listStr co dinh thuoc tap listKeyCan thi
							// phai tinh lai suc anh huong tu dinh keyCan toi
							// vertex.getName()
							// infValue.divide(this.mapInfValuePath.get(listStr));
							System.out.println("Duong di can loai bo: "
									+ listStr);
							BigDecimal infPathDivide = calculateInfluenceByPath(
									vertices, edges, listStr);
							// System.out
							// .println("infPathDivide=" + infPathDivide);
							if (infPathDivide.compareTo(BigDecimal.ZERO) != 0) {
								infValue = infValue.divide(
										BigDecimal.ONE.subtract(infPathDivide),
										20, BigDecimal.ROUND_HALF_UP); // round
																		// with
																		// 10
																		// digits
																		// of
																		// decimal
							}
							// System.out
							// .println("infValue after /=: " + infValue);
							countRemovePath++;
						}
					}
					// System.out.println("countRemovePath=" + countRemovePath);
					// System.out.println("size of mapPath=" +
					// this.mapInfluencePath.get(
					// keyCan).get(vertex).size());
					if (infValue.compareTo(BigDecimal.ONE) != 0
							|| this.mapInfluencePath.get(keyCan).get(vertex)
									.size() == countRemovePath) {
						// System.out.println("keyCan=" + keyCan);
						// System.out.println("vertex=" + vertex);
						// System.out.println("BigDecimal.ONE.subtract(infValue)="
						// + BigDecimal.ONE.subtract(infValue));
						// inf = new HashMap<String, BigDecimal>();
						Map<String, BigDecimal> infTemp = new HashMap<String, BigDecimal>();
						inf.put(vertex, BigDecimal.ONE.subtract(infValue));
						infTemp.putAll(inf);
						influenceValue.put(keyCan, infTemp);
						// System.out.println( keyCan + "-" + vertex + "=" +
						// influenceValue.get(keyCan).get(vertex));
						if (BigDecimal.ONE.subtract(infValue).compareTo(
								BigDecimal.ZERO) == 1) {
							// System.out.println("vertex=" + vertex);
							influentVertex.add(vertex);
						}
						// System.out.println("influentVertex1=" +
						// influentVertex);
					}
				}
				// System.out.println("keyCan1=" + keyCan);
				// System.out.println("influentVertex=" + influentVertex);

				influentVertices.put(keyCan, influentVertex);
			}
		}
		// Calculate influence from set of keyCandidates to every influented
		// node
		List<String> strSetUnion = new ArrayList<>();
		List<String> strSetUnionRemove = new ArrayList<>();
		for (String stt : keyCandidates) {
			// System.out.println("sttke=" + stt);
			if (influentVertices.get(stt) != null) {
				// System.out.println(influentVertices.get(stt));
				strSetUnion = union2List(strSetUnion, influentVertices.get(stt));
				strSetUnionRemove = union2ListRemoveDuplicate(strSetUnion,
						influentVertices.get(stt));
			}
		}
		// System.out.println("strSetUnion=" + strSetUnion);
		System.out.println("Cac dinh chiu anh huong cua tap: "
				+ strSetUnionRemove);
		System.out.println("Suc anh huong cua tap doi voi cac dinh la:");
		for (String stt : strSetUnionRemove) { // strSetUnionRemove la tap
												// chiu anh huong cua tap
												// keyCandidates
			if (countStringInList(strSetUnion, stt) == 1) {
				for (String str : keyCandidates) {
					// System.out.println("str=" + str);
					// System.out.println("stt=" + stt);
					if (influentVertices.get(str) != null) {
						if (influentVertices.get(str).contains(stt)) {
							if (influenceValue.get(str).get(stt)
									.compareTo(BigDecimal.ZERO) == 1) {
								influenceOfSetVertices = influenceOfSetVertices
										.add(influenceValue.get(str).get(stt));
								// System.out.println("S" +
								// influenceOfSetVertices );
							}
						}
					}
				}
			} else if (countStringInList(strSetUnion, stt) > 1) {
				BigDecimal influeValue = BigDecimal.ZERO;
				for (String str : keyCandidates) {
					if (influentVertices.get(str) != null) {
						if (influentVertices.get(str).contains(stt)) {
							// System.out.println(str + "=aa="+ stt +
							// influenceValue.get(str).get(stt));
							if (influenceValue.get(str).get(stt)
									.compareTo(BigDecimal.ZERO) == 1) {
								if (influeValue.compareTo(BigDecimal.ZERO) == 0) {
									influeValue = BigDecimal.ONE
											.subtract(influenceValue.get(str)
													.get(stt));
								} else {
									influeValue = influeValue
											.multiply(BigDecimal.ONE
													.subtract(influenceValue
															.get(str).get(stt)));
								}
							}
						}
					}
				}
				influenceOfSetVertices = influenceOfSetVertices
						.add(BigDecimal.ONE.subtract(influeValue));
				// System.out.println("S2" + influenceOfSetVertices );
			}
		}
		// System.out.println("key candidatessss=" + keyCandidates);
		System.out.println("Suc anh huong cua tap: " + influenceOfSetVertices);
		return influenceOfSetVertices;
	}

	private int CalculateInfluenceWithThreshold(List<Vertex> vertices,
			List<Edge> edges, List<String> keyCandidates) {
		Map<String, Map<String, BigDecimal>> influenceValue = new HashMap<String, Map<String, BigDecimal>>();
		Map<String, List<String>> influentVertices = new HashMap<String, List<String>>();
		Map<String, BigDecimal> inf = new HashMap<String, BigDecimal>();
		Map<String, Integer> flagCheckNode = new HashMap<String, Integer>();
		// System.out.println("keyCandidates=" + keyCandidates);
		for (String keyC : keyCandidates) {
			for (String ver : this.mapInfluented.get(keyC)) {
				// System.out.println("keyC" + keyC);
				// System.out.println("ver" + ver);
				// System.out.println("this.mapResult.get(keyC).get(ver)="
				// + this.mapResult.get(keyC).get(ver));
				if (this.mapResult.get(keyC).get(ver).compareTo(Data.theta) == 1) {
					int temp = 0;
					if (flagCheckNode.get(ver) != null) {
						temp = flagCheckNode.get(ver);
						flagCheckNode.remove(ver);
					}
					// System.out.println("Temp=" + temp);
					flagCheckNode.put(ver, temp + 1);
				}
				else {
					int temp = 0;
					if (flagCheckNode.get(ver) != null) {
						temp = flagCheckNode.get(ver);
						flagCheckNode.remove(ver);
					}
					// System.out.println("Temp=" + temp);
					flagCheckNode.put(ver, temp);
				}
			}
		}
//		System.out.println("flagCheckNode=" + flagCheckNode.get("1"));
		for (String keyCan : keyCandidates) {
			List<String> listKeyCan = new ArrayList<String>();
			for (String st : keyCandidates) {
				listKeyCan.add(st);
			}
			listKeyCan.remove(keyCan); // tap S \ dinh dang xet suc anh huong
			BigDecimal infValue = BigDecimal.ZERO;
			List<String> influentVertex = new ArrayList<String>();
			// System.out.println("this.mapInfluented.get(keyCan)=" +
			// this.mapInfluented.get(keyCan));
			for (String vertex : this.mapInfluented.get(keyCan)) {
//				System.out.println("keyCan" + keyCan);
//				System.out.println("vertex" + vertex);
//				System.out.println("flagCheckNode.get(vertex)="
//						+ flagCheckNode.get(vertex));
//				System.out.println("keyCandidates.size()="
//						+ keyCandidates.size());
				if (flagCheckNode.get(vertex) != null) {
					if (flagCheckNode.get(vertex) < keyCandidates.size()) {
						// System.out.println("keyCan=" + keyCan);
						// System.out.println("vertex=" + vertex);
						infValue = BigDecimal.ONE.subtract(this.mapResult.get(
								keyCan).get(vertex));
						// System.out.println("infValue=" + infValue);
						if (this.mapInfluencePath != null) {
							int countRemovePath = 0;
							for (List<String> listStr : this.mapInfluencePath
									.get(keyCan).get(vertex)) {
								// System.out.println("listStr: " + listStr);
								if (!Collections.disjoint(listKeyCan, listStr)) {
									// Duong di listStr co dinh thuoc tap
									// listKeyCan
									// thi
									// phai tinh lai suc anh huong tu dinh
									// keyCan
									// toi
									// vertex.getName()
									// infValue.divide(this.mapInfValuePath.get(listStr));
									System.out.println("Duong di can loai bo: "
											+ listStr);
									BigDecimal infPathDivide = calculateInfluenceByPath(
											vertices, edges, listStr);
									// System.out
									// .println("infPathDivide=" +
									// infPathDivide);
									if (infPathDivide
											.compareTo(BigDecimal.ZERO) != 0) {
										infValue = infValue
												.divide(BigDecimal.ONE
														.subtract(infPathDivide),
														20,
														BigDecimal.ROUND_HALF_UP); // round
																					// with
																					// 10
																					// digits
																					// of
																					// decimal
									}
									// System.out
									// .println("infValue after /=: " +
									// infValue);
									countRemovePath++;
								}
							}
							// System.out.println("countRemovePath=" +
							// countRemovePath);
							// System.out.println("size of mapPath=" +
							// this.mapInfluencePath.get(
							// keyCan).get(vertex).size());
							if (infValue.compareTo(BigDecimal.ONE) != 0
									|| this.mapInfluencePath.get(keyCan)
											.get(vertex).size() == countRemovePath) {
								// System.out.println("keyCan=" + keyCan);
								// System.out.println("vertex=" + vertex);
								// System.out.println("BigDecimal.ONE.subtract(infValue)="
								// + BigDecimal.ONE.subtract(infValue));
								// inf = new HashMap<String, BigDecimal>();
								Map<String, BigDecimal> infTemp = new HashMap<String, BigDecimal>();
								inf.put(vertex,
										BigDecimal.ONE.subtract(infValue));
								infTemp.putAll(inf);
								influenceValue.put(keyCan, infTemp);
								// System.out.println( keyCan + "-" + vertex +
								// "=" +
								// influenceValue.get(keyCan).get(vertex));
								if (BigDecimal.ONE.subtract(infValue)
										.compareTo(BigDecimal.ZERO) == 1) {
									// System.out.println("vertex=" + vertex);
									influentVertex.add(vertex);
								}
								// System.out.println("influentVertex1=" +
								// influentVertex);
							}
						}
						// System.out.println("keyCan1=" + keyCan);
						// System.out.println("influentVertex=" +
						// influentVertex);

						influentVertices.put(keyCan, influentVertex);
					} else {
						influentVertex.add(vertex);
						influentVertices.put(keyCan, influentVertex);
					}
				}
			}
		}
		// Calculate influence from set of keyCandidates to every influented
		// node
		List<String> strSetUnion = new ArrayList<String>();
		List<String> strSetUnionRemove = new ArrayList<String>();
		for (String stt : keyCandidates) {
			// System.out.println("sttke=" + stt);
			if (influentVertices.get(stt) != null) {
//				System.out.println(stt+ "=" + influentVertices.get(stt));
				// System.out.println(influentVertices.get(stt));
				strSetUnion = union2List(strSetUnion, influentVertices.get(stt));
				strSetUnionRemove = union2ListRemoveDuplicate(
						strSetUnionRemove, influentVertices.get(stt));
			}
		}
//		System.out.println("strSetUnion=" + strSetUnion);
		System.out.println("Cac dinh chiu anh huong cua tap: "
				+ strSetUnionRemove);
		System.out.println("Suc anh huong cua tap doi voi cac dinh la:");
		int countOk = 0;
//		System.out.println("flagCheckNode22=" + flagCheckNode.get("1"));
		for (String stt : strSetUnionRemove) { // strSetUnionRemove la tap
												// chiu anh huong cua tap
												// keyCandidates
//			if (flagCheckNode.get(stt) != null) {
//				System.out.println("flagCheckNode.get(stt)="
//						+ flagCheckNode.get(stt));
//				System.out.println("keyCandidates.size()="
//						+ keyCandidates.size());
//			}
			if (flagCheckNode.get(stt) != null) {
				if (flagCheckNode.get(stt) == keyCandidates.size()) {
					System.out.println(stt + ": --> Vuot nguong " + Data.theta);
					countOk++;
				} else {
					if (countStringInList(strSetUnion, stt) == 1) {
						for (String str : keyCandidates) {
							// System.out.println("str=" + str);
							// System.out.println("stt=" + stt);
							if (influentVertices.get(str) != null) {
								if (influentVertices.get(str).contains(stt)) {
									if (influenceValue.get(str).get(stt)
											.compareTo(Data.theta) == 1) {
										System.out.println(stt
												+ ": "
												+ String.valueOf(influenceValue
														.get(str).get(stt))
												+ " --> Vuot nguong "
												+ Data.theta);
										countOk++;
									} else {
										System.out.println(stt
												+ ": "
												+ String.valueOf(influenceValue
														.get(str).get(stt))
												+ " --> Khong Vuot nguong "
												+ Data.theta);
									}
								}
							}
						}
					} else if (countStringInList(strSetUnion, stt) > 1) {
						BigDecimal influeValue = BigDecimal.ZERO;
						for (String str : keyCandidates) {
							if (influentVertices.get(str) != null) {
								if (influentVertices.get(str).contains(stt)) {
									// System.out.println(str + "=aa="+ stt +
									// influenceValue.get(str).get(stt));
									if (influenceValue.get(str).get(stt)
											.compareTo(BigDecimal.ZERO) == 1) {
										if (influeValue
												.compareTo(BigDecimal.ZERO) == 0) {
											influeValue = BigDecimal.ONE
													.subtract(influenceValue
															.get(str).get(stt));
										} else {
											influeValue = influeValue
													.multiply(BigDecimal.ONE
															.subtract(influenceValue
																	.get(str)
																	.get(stt)));
										}
									}
								}
							}
						}

						if (BigDecimal.ONE.subtract(influeValue).compareTo(
								Data.theta) == 1) {
							System.out.println(stt + ": "
									+ BigDecimal.ONE.subtract(influeValue)
									+ " --> Vuot nguong " + Data.theta);
							countOk++;
						} else if (BigDecimal.ONE.subtract(influeValue)
								.compareTo(Data.theta) == -1
								&& BigDecimal.ONE.subtract(influeValue)
										.compareTo(BigDecimal.ZERO) == 1) {
							System.out.println(stt + ": "
									+ BigDecimal.ONE.subtract(influeValue)
									+ " --> Khong Vuot nguong " + Data.theta);
						}
					}
				}
			} else {
				countOk = 0;
			}
		}
		return countOk;
	}

	public JavaPairRDD<String, List<String>> getIndirectInfluence(
			List<Vertex> vertices, List<Edge> edges) {

		if (this.indirectInfluence == null) {
			getAllInfluenceOfVertices(vertices, edges);
		}

		if (this.indirectInfluence.count() > 1 && !Data.flagSorted) {
			indirectInfluenceCount = indirectInfluence.mapToPair(tuple -> {
				return new Tuple2<Integer, String>(tuple._2.size(), tuple._1);
			}).sortByKey(false).mapToPair(tuple -> tuple.swap()).collect();
			Data.flagSorted = true;
		}

		return this.indirectInfluence;
	}

	public String getTheMostOverThresholdVertexName(List<Vertex> vertices,
			List<Edge> edges) {
		if (indirectInfluenceCount == null) {
			getAllInfluenceOfVertices(vertices, edges);
		}
		return indirectInfluenceCount.get(0)._1;
	}

	public List<String> getSmallestGroup(List<Vertex> vertices, List<Edge> edges) {
		int iOrgSize = vertices.size();

		getIndirectInfluence(vertices, edges);

		int iEdgesCount = edges.size();

		for (int i = 1; i < iOrgSize; i++) {
			if (result == null) {
				getCombinations(i, iEdgesCount);
			} else {
				break;
			}
		}

		System.out.println("So to hop phai duyet qua la: " + lCount);

		return result;
	}

	public void getCombinations(int k, int iMax) {
		int n = iMax;
		int a[] = new int[((int) n + 1)];
		for (int t = 1; t <= k; t++) {
			a[t] = t;
			System.out.println("a[" + t + "]=" + a[t]);
		}
		int i = 0;
		do {
			lCount++;
			// PrintCombine(a, k);
			int iTong = 0;
			for (int l = 1; l <= k; l++) {
				// iTong la so dinh tong cong cua to hop
				iTong += indirectInfluenceCount.get(a[l] - 1)._2;
			}
			if (iTong >= Data.iNeed) {
				List<String> lMem = new ArrayList<String>();
				for (int l = 1; l <= k; l++) {
					// str là đỉnh được xét với số đỉnh chịu ảnh hưởng của nó
					// vượt ngưỡng là lTemp
					String str = indirectInfluenceCount.get(a[l] - 1)._1;
					List<String> lTemp = indirectInfluence.lookup(str).get(0);
					System.out.println("lTemp" + lTemp);
					for (String string : lTemp) {
						if (!lMem.contains(string)) {
							lMem.add(string);
						}
					}
				}
				if (lMem.size() >= Data.iNeed) {
					// System.out.println("Được");
					// if (result == null) {
					result = new ArrayList<String>(k);
					for (int l = 1; l <= k; l++) {
						result.add(indirectInfluenceCount.get(a[l] - 1)._1);
					}
					// }
					break;
				} else {
					// System.out.println("Không được");
				}
			} else {
				// System.out.println("Không được và cắt");
				n = a[k] - 1;
			}
			i = k;
			while ((i > 0) && (a[i] >= n - k + i)) {
				--i;
			}
			if (i > 0) {
				a[i]++;
				for (int j = i + 1; j <= k; j++) {
					a[j] = a[j - 1] + 1;
				}
			}
		} while (i != 0 && result == null);
	}

	private static Map<String, List<String>> sortByComparator(
			Map<String, List<String>> unsortMap, final boolean order) {
		List<Entry<String, List<String>>> list = new LinkedList<Entry<String, List<String>>>(
				unsortMap.entrySet());

		// Sorting the list based on values
		Collections.sort(list, new Comparator<Entry<String, List<String>>>() {
			public int compare(Entry<String, List<String>> o1,
					Entry<String, List<String>> o2) {
				if (order) {
					return String.valueOf(o1.getValue().size()).compareTo(
							String.valueOf(o2.getValue().size()));
				} else {
					return String.valueOf(o2.getValue().size()).compareTo(
							String.valueOf(o1.getValue().size()));
				}
			}
		});

		// Maintaining insertion order with the help of LinkedList
		Map<String, List<String>> sortedMap = new LinkedHashMap<String, List<String>>();
		for (Entry<String, List<String>> entry : list) {
			sortedMap.put(entry.getKey(), entry.getValue());
		}

		return sortedMap;
	}

	public static void printMap(Map<String, List<String>> map) {
		for (Entry<String, List<String>> entry : map.entrySet()) {
			System.out.println("Key : " + entry.getKey() + " Value : "
					+ entry.getValue().size());
		}
	}

	// generate actual subset by index sequence
	private static int[] getSubset(int[] input, int[] subset) {
		int[] result = new int[subset.length];
		for (int i = 0; i < subset.length; i++)
			result[i] = input[subset[i]];
		return result;
	}

	private boolean checkContain2List(List<String> list1, List<String> list2) {
		for (String l2 : list2) {
			if (list1.contains(l2))
				return false;
		}
		return true;
	}

	private static String[] addVertexAfterSorted(
			Map<String, List<String>> mapInfluented) {
		String[] arrayResult = new String[mapInfluented.size() + 1];
		List<String> indexes = new ArrayList<String>(mapInfluented.keySet());
		for (Entry<String, List<String>> entry : mapInfluented.entrySet()) {
			// System.out.println("Key : " + entry.getKey() + " Value : "
			// + entry.getValue().size());
			arrayResult[indexes.indexOf(entry.getKey()) + 1] = String
					.valueOf(entry.getKey());
			// System.out.println("getIndex="
			// + String.valueOf(indexes.indexOf(entry.getKey()) + 1));
			// System.out.println("getKey=" + String.valueOf(entry.getKey()));
		}
		return arrayResult;
	}

	private static List<int[]> findCombinations(List<Vertex> vertices,
			int[] inputArray, int k) {
		List<int[]> subsets = new ArrayList<>();
		subsets = new ArrayList<>();
		int[] s = new int[k]; // here we'll keep indices
								// pointing to elements in input array
		if (k <= inputArray.length) {
			// first index sequence: 0, 1, 2, ...
			for (int i = 0; (s[i] = i) < k - 1; i++)
				;
			subsets.add(getSubset(inputArray, s));
			for (;;) {
				int i;
				// find position of item that can be incremented
				for (i = k - 1; i >= 0 && s[i] == inputArray.length - k + i; i--)
					;
				if (i < 0) {
					break;
				} else {
					s[i]++; // increment this item
					for (++i; i < k; i++) { // fill up remaining items
						s[i] = s[i - 1] + 1;
					}
					subsets.add(getSubset(inputArray, s));
				}
			}
		}
		return subsets;
	}

	private int CheckThresholdSetOneNode(String strS) {
		int count = 0;
		for (String vertex : this.mapInfluented.get(strS)) {
			if (this.mapResult.get(strS) != null) {
				if (this.mapResult.get(strS).get(vertex).compareTo(Data.theta) == 1) {
					count++;
				}
			}
		}
		return count;
	}

	public List<String> findSmallestGroup(List<Vertex> vertices,
			JavaRDD<Vertex> rddVertices, List<Edge> edges) {
		int[] inputArray = new int[vertices.size()];
		List<int[]> subsets = new ArrayList<int[]>();
		List<String> strSetS = new ArrayList<String>();
		String strS = new String();
		String[] arrayVertex = new String[vertices.size() + 1];
		// boolean flagStopNextSet = false;
		for (int i = 1; i <= vertices.size(); i++) {
			inputArray[i - 1] = i;
		}

		rddVertices.cache();

		for (int k = 1; k <= vertices.size(); k++) {
			subsets = findCombinations(vertices, inputArray, k);
			// flagStopNextSet = false;
			// if (subsets.size() == vertices.size()) { // moi tap co 1 dinh
			// System.out.println("subsets length=" + subsets.get(0).length);
			if (subsets.get(0).length == 1) { // moi tap co 1 dinh
				for (int[] strings : subsets) {
					strS = new String();
					// System.out.println("a" + Arrays.toString(strings));

					strS = String.valueOf(strings[0]);

					System.out.println("***Xet tap: [" + strS + "]");
					m_countChecking++;
					// for (String stt : strSetS) {

					IndirectInfluenceOfVertexOnAllVertex(vertices, rddVertices,
							edges, strS);

					// InfluenceOfSetVertexOnAllVertex(vertices, rddVertices,
					// edges, strSetS, stt);
					// System.out.println("Dinh " + stt
					// + " anh huong toi cac dinh: "
					// + this.mapInfluented.get(stt));
					// }
					// int count = CalculateInfluenceOfSet(strSetS);
					int count = CheckThresholdSetOneNode(strS);
					if (count >= Data.iNeed) {
						System.out.println(" => Tap nay thoa dieu kien\n");
						System.out.println("So to hop phai duyet: "
								+ m_countChecking);
						return strSetS;
					} else {
						System.out
								.println(" => Tap nay khong thoa dieu kien\n");
					}
				}

				this.mapInfluented = sortByComparator(this.mapInfluented, DESC);
				arrayVertex = addVertexAfterSorted(this.mapInfluented);
			} else {
				for (int[] strings : subsets) { // moi tap co so dinh >= 2
					strSetS = new ArrayList<String>();
					// System.out.println("a" + Arrays.toString(strings));
					for (int jj = 0; jj < strings.length; jj++) {
						// System.out.println("String=" +
						// String.valueOf(arrayVertex[strings[jj]]));
						strSetS.add(String.valueOf(arrayVertex[strings[jj]]));
					}
					// System.out.println("***: " + strSetS);
					// Xet xem tap dang xet co thoa dieu kien, tong so dinh chiu
					// anh huong > Data.iNeed hay khong. Neu khong se ko xet den
					// cac tap co cung so luong dinh tiep theo nua ma chuyen
					// sang xet tap dinh co nhieu hon 1 phan tu
					List<String> strSetUnion = new ArrayList<String>();
					for (String stt : strSetS) {
						// System.out.println("this.mapInfluentedFirst.get(stt)="
						// + this.mapInfluentedFirst.get(stt));
						strSetUnion = union2List(strSetUnion,
								this.mapInfluented.get(stt));
					}

					// for (String stt : strSetUnionRemove) {
					// System.out.println("stt=" + stt);
					// }
					// System.out.println("strSetUnionRemove.size()=" +
					// strSetUnionRemove.size());
//					System.out.println("strSetUnion1=" + strSetUnion);
					if (strSetUnion.size() < Data.iNeed) {
						// flagStopNextSet = true;
						break;
					}

					System.out.println("***Xet tap: " + strSetS);
					m_countChecking++;
					// for (String stt : strSetS) {
					// if (strSetS.size() > 1
					// && checkContain2List(
					// this.mapInfluented.get(stt),
					// strSetS) == true) {
					// this.mapInfluented.put(stt,
					// this.mapInfluentedFirst.get(stt));
					// this.mapResult.put(stt,
					// this.mapResultFirst.get(stt));
					// } else
					// {
					// System.out.println(stt);
					int count = CalculateInfluenceWithThreshold(vertices,
							edges, strSetS);
					if (count >= Data.iNeed) {
						System.out.println(" => Tap nay thoa dieu kien\n");
						System.out.println("So to hop phai duyet: "
								+ m_countChecking);
						return strSetS;
					} else {
						System.out
								.println(" => Tap nay khong thoa dieu kien\n");
					}
					// }
					// System.out.println("Dinh " + stt
					// + " anh huong toi cac dinh: "
					// + this.mapInfluented.get(stt));
					// }
					// int count = CalculateInfluenceOfSet(strSetS);
				}
			}
		}
		System.out.println("So to hop phai duyet: " + m_countChecking);
		return new ArrayList<String>();
	}

	private static boolean stringContainsItemFromList(String inputStr,
			List<String> items) {
		return items.stream().anyMatch(str -> str.trim().equals(inputStr));
	}

	public List<String> findSetKeyPlayers(List<Vertex> vertices,
			List<Edge> edges) {
		int[] inputArray = new int[vertices.size()];
		BigDecimal influenceResult = BigDecimal.ZERO;
		for (int i = 1; i <= vertices.size(); i++) {
			inputArray[i - 1] = i;
		}
		String keyOneVertex = findKeyPlayer(vertices, edges);
		keyPlayerSet = new ArrayList<>();
		keyPlayerSet.add(keyOneVertex);
		for (int ii = 2; ii <= Data.iNeed; ii++) {
			JavaPairRDD<String, BigDecimal> all = getAllInfluenceOfSetVertices(
					vertices, edges, keyPlayerSet);
			all.cache();
			List<Tuple2<String, BigDecimal>> listAll = all.collect();
			// for (Tuple2<String, BigDecimal> tuple : listAll) {
			// System.out.println("[ " + tuple._1 + " : " + tuple._2.toString()
			// + " ]");
			// }

			// System.out.println("Key Player la: ");
			Tuple2<String, BigDecimal> kp = all.first();
			System.out.println(kp._1 + ": " + kp._2.toString());
			keyPlayerSet.add(kp._1);
			influenceResult = new BigDecimal(kp._2.toString());
		}
		return this.keyPlayerSet;
	}

	private static List<String> union2List(List<String> list1,
			List<String> list2) {
		List<String> set = new ArrayList<>();

		set.addAll(list1);
		set.addAll(list2);

		return new ArrayList<String>(set);
	}

	private static List<String> union2ListRemoveDuplicate(List<String> list1,
			List<String> list2) {
		HashSet<String> set = new HashSet<>();

		set.addAll(list1);
		set.addAll(list2);

		return new ArrayList<String>(set);
	}

	private int countStringInList(List<String> listString, String string) {
		int count = 0;
		for (String s : listString) {
			if (string.equals(s))
				count++;
		}
		return count;
	}

	private int CalculateInfluenceOfSet(List<String> strSet) {
		List<String> strSetUnion = new ArrayList<>();
		List<String> strSetUnionRemove = new ArrayList<>();
		for (String stt : strSet) {
			strSetUnion = union2List(strSetUnion, this.mapInfluented.get(stt));
			strSetUnionRemove = union2ListRemoveDuplicate(strSetUnion,
					this.mapInfluented.get(stt));
		}
		if (strSetUnionRemove.size() < Data.iNeed) {
			return 0;
		}
		System.out.println("Suc anh huong cua tap doi voi cac dinh la:");
		int countOk = 0;
		for (String stt : strSetUnionRemove) { // strSetUnionRemove la tap bi
												// anh
												// huong cua
												// tap strSet
			if (countStringInList(strSetUnion, stt) == 1) {
				for (String str : strSet) {
					if (!this.mapResult.isEmpty()) {
						if (this.mapResult.get(str).get(stt)
								.compareTo(Data.theta) == 1) {
							System.out.println(stt
									+ ": "
									+ String.valueOf(this.mapResult.get(str)
											.get(stt)) + " --> Vuot nguong "
									+ Data.theta);
							countOk++;
						} else if (this.mapResult.get(str).get(stt)
								.compareTo(Data.theta) == -1
								&& mapResult.get(str).get(stt)
										.compareTo(BigDecimal.ZERO) == 1) {
							System.out.println(stt
									+ ": "
									+ String.valueOf(this.mapResult.get(str)
											.get(stt))
									+ " --> Khong Vuot nguong " + Data.theta);
						}
					}
				}
			} else if (countStringInList(strSetUnion, stt) > 1) {
				BigDecimal influentedValue = BigDecimal.ZERO;
				for (String str : strSet) {
					if (!this.mapResult.isEmpty()) {
						if (this.mapResult.get(str).get(stt)
								.compareTo(BigDecimal.ZERO) == 1) {
							if (influentedValue.compareTo(BigDecimal.ZERO) == 0) {
								influentedValue = BigDecimal.ONE
										.subtract(this.mapResult.get(str).get(
												stt));
							} else {
								influentedValue = influentedValue
										.multiply(BigDecimal.ONE
												.subtract(this.mapResult.get(
														str).get(stt)));
							}
						}
					}
				}
				if (BigDecimal.ONE.subtract(influentedValue).compareTo(
						Data.theta) == 1) {
					System.out.println(stt + ": "
							+ BigDecimal.ONE.subtract(influentedValue)
							+ " --> Vuot nguong " + Data.theta);
					countOk++;
				} else if (BigDecimal.ONE.subtract(influentedValue).compareTo(
						Data.theta) == -1
						&& BigDecimal.ONE.subtract(influentedValue).compareTo(
								BigDecimal.ZERO) == 1) {
					System.out.println(stt + ": "
							+ BigDecimal.ONE.subtract(influentedValue)
							+ " --> Khong Vuot nguong " + Data.theta);
				}
			}
		}
		return countOk;
	}

	private BigDecimal CalculateInfluenceOfSetKeyPlayers(List<String> strSet) {
		List<String> strSetUnion = new ArrayList<>();
		List<String> strSetUnionRemove = new ArrayList<>();
		BigDecimal influenceOfSet = BigDecimal.ZERO;
		for (String stt : strSet) {
			strSetUnion = union2List(strSetUnion, this.mapInfluented.get(stt));
			strSetUnionRemove = union2ListRemoveDuplicate(strSetUnion,
					this.mapInfluented.get(stt));
		}
		if (strSetUnionRemove.size() > 0) {
			System.out.println("Suc anh huong cua tap doi voi cac dinh la:");
			for (String stt : strSetUnionRemove) { // strSetUnionRemove la tap
													// bi
													// anh
													// huong cua
													// tap strSet
				if (countStringInList(strSetUnion, stt) == 1) {
					for (String str : strSet) {
						if (!mapResult.isEmpty()) {
							influenceOfSet = influenceOfSet.add(mapResult.get(
									str).get(stt));
							if (mapResult.get(str).get(stt)
									.compareTo(BigDecimal.ZERO) == 1) {
								System.out.println(str + "->" + stt + ":"
										+ mapResult.get(str).get(stt));
							}
						}
					}
				} else if (countStringInList(strSetUnion, stt) > 1) {
					BigDecimal influentedValue = BigDecimal.ZERO;
					for (String str : strSet) {
						if (!mapResult.isEmpty()) {
							if (mapResult.get(str).get(stt)
									.compareTo(BigDecimal.ZERO) == 1) {
								if (influentedValue.compareTo(BigDecimal.ZERO) == 0) {
									influentedValue = BigDecimal.ONE
											.subtract(mapResult.get(str).get(
													stt));
								} else {
									influentedValue = influentedValue
											.multiply(BigDecimal.ONE
													.subtract(mapResult
															.get(str).get(stt)));
								}
								if (mapResult.get(str).get(stt)
										.compareTo(BigDecimal.ZERO) == 1) {
									System.out.println(str + "->" + stt + ":"
											+ mapResult.get(str).get(stt));
								}
							}
						}
					}
					influenceOfSet = influenceOfSet.add(BigDecimal.ONE
							.subtract(influentedValue));
				}
			}
		}
		return influenceOfSet;
	}

	/*
	 * private void cloneStringList(List<String> scr, List<String> des) { for
	 * (String item : scr) { des.add(item); } }
	 */

	public String GraphToString(List<Vertex> vertices, List<Edge> edges) {
		String sResult = new String("Vertices:");
		for (Vertex vertex : vertices) {
			sResult += "\nName:" + vertex.getName();
			// sResult += "\nSpreadCoefficiency: {\n";
			Map<String, BigDecimal> msc = vertex.getSpreadCoefficient();
			if (msc != null && !msc.isEmpty()) {
				sResult += Arrays.toString(msc.entrySet().toArray());
			}
			// sResult += "\n}";
		}
		sResult += "\nEdges:\n";
		for (Edge edge : edges) {
			sResult += "Start: " + edge.getStartVertexName() + ", End: "
					+ edge.getEndVertexName() + ", DirectInfluence: "
					+ edge.getDirectInfluence().toString() + "\n";
		}
		return sResult;
	}

	public List<Segment> getSegmentFromEdges(List<Vertex> vertices,
			List<Edge> edges) {
		JavaRDD<Edge> rddEdges = sc.parallelize(edges);
		final Broadcast<List<Vertex>> bcVertices = sc.broadcast(vertices);

		JavaRDD<Segment> rddSegments = rddEdges
				.map(edge -> {
					String sEndVertex = edge.getEndVertexName();
					String sStartVertex = edge.getStartVertexName();
					List<Vertex> tempList = bcVertices.value();
					BigDecimal bd = BigDecimal.ZERO;
					for (Vertex vertex : tempList) {
						if (vertex.getName().equals(sEndVertex)) {
							System.out
									.println("Spread: "
											+ vertex.getSpreadCoefficientFromVertexName(
													sStartVertex).toString());
							if (vertex
									.getSpreadCoefficientFromVertexName(
											sStartVertex).toString()
									.equals("1")) {
								bd = edge
										.getDirectInfluence()
										.multiply(
												vertex.getSpreadCoefficientFromVertexName(sStartVertex));
								break;
							} else {
								bd = edge.getDirectInfluence();
								break;
							}
						}
					}
					return new Segment(sStartVertex, sEndVertex, bd);
				});

		return rddSegments.collect();
	}

	public List<Segment> getPathFromSegment(JavaRDD<Segment> rddSegments,
			Broadcast<List<Segment>> bcOneSegmentList,
			Broadcast<List<Vertex>> bcVertices) {
		JavaRDD<Segment> rddResult = rddSegments.flatMap(seg -> {
			// TODO Auto-generated method stub
				List<Segment> listResult = new ArrayList<Segment>();
				List<Segment> listOneSegment = bcOneSegmentList.value();
				String sSeqEndVertex = seg.getEndVertex();
				String sSegStartVertex = seg.getStartVertex();
				ArrayList<String> segArrHistory = (ArrayList<String>) seg
						.getHistory().clone();
				for (Segment s : listOneSegment) {
					String sStart = s.getStartVertex();
					String sEnd = s.getEndVertex();
					if ((sStart.equals(sSeqEndVertex))
							&& !(segArrHistory.contains(sStart))
							&& !(segArrHistory.contains(sEnd))
							&& !(sEnd.equals(sSegStartVertex))) {
						segArrHistory.add(sStart);
						listResult.add(new Segment(sSegStartVertex, sEnd, seg
								.getIndirectInfluence().multiply(
										s.getIndirectInfluence()),
								segArrHistory));
					}
				}
				/*
				 * if (listResult.isEmpty()){ return null; } else{
				 */
				return listResult;
				// }
			});
		return rddResult.collect();
	}

	public BigDecimal getVertexIndirectInfluenceFromAllPath(
			List<Tuple2<String, BigDecimal>> allVertexPath) {
		if (!allVertexPath.isEmpty()) {
			JavaPairRDD<String, BigDecimal> rddVertexPath = sc
					.parallelizePairs(allVertexPath).mapToPair(
							(tuple0) -> {
								return new Tuple2<String, BigDecimal>(
										tuple0._1, BigDecimal.ONE
												.subtract(tuple0._2));
							});
			BigDecimal bdResult = rddVertexPath.reduceByKey((val0, val1) -> {
				return val0.multiply(val1);
			}).map(pair -> {
				return BigDecimal.ONE.subtract(pair._2);
			}).reduce((bd0, bd1) -> {
				return bd0.add(bd1);
			});

			return bdResult;
		} else {
			return BigDecimal.ZERO;
		}
	}

	private BigDecimal getVertexIndirectInfluenceFromMatrix(
			Map<String, Map<String, BigDecimal>> mapResult, String sVName) {
		if (!mapResult.isEmpty()) {
			BigDecimal bdResult = BigDecimal.ZERO;
			/*
			 * for (Entry<String[], BigDecimal> res : mapResult.entrySet()) { if
			 * (res.getKey()[0].equals(sVName)){ bdResult =
			 * bdResult.add(BigDecimal.ONE.subtract(res.getValue())); } }
			 */
			for (BigDecimal bd : mapResult.get(sVName).values()) {
				bdResult = bdResult.add(BigDecimal.ONE.subtract(bd));
			}

			return bdResult;
		} else {
			return BigDecimal.ZERO;
		}
	}

	public List<Tuple2<String, BigDecimal>> getAllVertexInInfFromMatrix(
			Broadcast<Map<String, Map<String, BigDecimal>>> bcMapResult,
			JavaRDD<Vertex> rddVertices) {
		return rddVertices.mapToPair(
				vertex -> {
					String sVName = vertex.getName();
					return new Tuple2<String, BigDecimal>(sVName,
							getVertexIndirectInfluenceFromMatrix(
									bcMapResult.getValue(), sVName));
				}).collect();
	}

	public Entry<String, BigDecimal> getKeyPlayerFromMatrix(
			Map<String, Map<String, BigDecimal>> mapResult,
			List<Vertex> vertices) {
		BigDecimal bdMax = BigDecimal.ZERO;
		String sKP = "";
		System.out.println("Suc anh huong cua tat ca cac dinh:");
		for (Vertex vertex : vertices) {
			BigDecimal bdVertexIndInf = BigDecimal.ZERO;
			/*
			 * for (Entry<String[], BigDecimal> res : mapResult.entrySet()) { if
			 * (res.getKey()[0].equals(vertex.getName())){ bdVertexIndInf =
			 * bdVertexIndInf.add(BigDecimal.ONE.subtract(res.getValue())); } }
			 */
			for (BigDecimal bd : mapResult.get(vertex.getName()).values()) {
				bdVertexIndInf = bdVertexIndInf
						.add(BigDecimal.ONE.subtract(bd));
			}

			System.out.println("[ " + vertex.getName() + ": "
					+ bdVertexIndInf.toPlainString() + " ]");
			if (bdVertexIndInf.compareTo(bdMax) == 1) {
				bdMax = bdVertexIndInf;
				sKP = vertex.getName();
			}
		}

		return new AbstractMap.SimpleEntry(sKP, bdMax);
	}

	public String findKeyPlayer(List<Vertex> vertices, List<Edge> edges) {
		JavaPairRDD<String, BigDecimal> all = getAllInfluenceOfVertices(
				vertices, edges);
		all.cache();

		// System.out.println("Suc anh huong cua tat ca cua dinh:");
		List<Tuple2<String, BigDecimal>> listAll = all.collect();
		// for (Tuple2<String, BigDecimal> tuple : listAll) {
		// System.out.println("[ " + tuple._1 + " : " + tuple._2.toString()
		// + " ]");
		// }

		// System.out.println("Key Player la: ");
		Tuple2<String, BigDecimal> kp = all.first();
		System.out.println(kp._1 + ": " + kp._2.toString());
		return kp._1;
	}

	public void findInfluenceEvery(List<Vertex> vertices, List<Edge> edges) {
		JavaPairRDD<String, BigDecimal> all = getAllInfluenceOfVertices(
				vertices, edges);
		all.cache();

		// System.out.println("Suc anh huong cua tat ca cua dinh:");
		// List<Tuple2<String, BigDecimal>> listAll = all.collect();
		// for (Tuple2<String, BigDecimal> tuple : listAll) {
		// System.out.println("[ " + tuple._1 + " : " + tuple._2.toString()
		// + " ]");
		// }

		// System.out.println("Key Player la: ");
		// Tuple2<String, BigDecimal> kp = all.first();
		// System.out.println(kp._1 + ": " + kp._2.toString());
		// return kp._1;
	}
}
