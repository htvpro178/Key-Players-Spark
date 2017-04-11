package uit.keyplayer;

import java.io.Serializable;
import java.math.BigDecimal;
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
import org.apache.spark.sql.types.Decimal;

import scala.Tuple2;

public class Utils implements Serializable {
	public static boolean ASC = true;
	public static boolean DESC = false;
	// private Broadcast<JavaRDD<Vertex>> bcVertices;
	// private Broadcast<JavaRDD<Edge>> bcEdges;
	// private List<Vertex> vertices;
	// private List<Edge> edges;
	private JavaPairRDD<String, List<String>> indirectInfluence;
	private List<Tuple2<String, Integer>> indirectInfluenceCount;
	// private JavaPairRDD<String, List<String>> influence;
	private Map<String, Map<String, BigDecimal>> mapResult;
	private Map<String, List<String>> mapInfluented;
	private Map<String, Map<String, BigDecimal>> mapResultFirst;
	private Map<String, List<String>> mapInfluentedFirst;
	private ArrayList<String> result;
	private long lCount; // dem so luong to hop phai duyet qua, old b3, not use
	// private long lCountChecking; // dem so luong to hop phai duyet qua, old
	// b3, not use
	private long m_countChecking;
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
		this.mapInfluentedFirst = new HashMap<String, List<String>>();
		this.m_countChecking = 0;
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
			return new BigDecimal("1");
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

	public BigDecimal IndirectInfluenceOfVertexOnOtherVertex(
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
															// da tham

		List<String> endpath = new ArrayList<String>();
		endpath.add(sEndName);
		boolean fChangeEnd = false;
		List<Edge> whetherOneEdge = getEdgesEndAtVertex(edges, sEndName);
		while (whetherOneEdge.size() <= 1) {
			if (whetherOneEdge.size() == 0) {
				System.out.println("Khong co duong di tu dinh " + sStartName
						+ " den dinh " + sEndName);
				return BigDecimal.ZERO;
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
								System.out
										.println("Duong di duy nhat vua duoc tinh truoc khi ngat: "
												+ endpath);
								return BigDecimal.ONE;
							}
						}
						sBefore = v;
					}
					System.out.println("Duong di duy nhat vua duoc tinh la: "
							+ endpath);
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
						System.out
								.println("Duong di vua duoc tinh la: " + temp);

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
		return BigDecimal.ONE.subtract(fIndirectInfluence);
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
		/*
		 * List<String> OverThresholdVertex = new ArrayList<String>();
		 * 
		 * for (Vertex vertex : vertices) { String vName = vertex.getName(); if
		 * (!vName.equals(sVertexName)) { BigDecimal bd =
		 * IndirectInfluenceOfVertexOnOtherVertex(vertices, edges, sVertexName,
		 * vName); fIndirectInfluence = fIndirectInfluence.add(bd); if
		 * (bd.compareTo(Data.theta) != -1) { OverThresholdVertex.add(vName); }
		 * } }
		 * 
		 * if (indirectInfluence != null) { if
		 * (indirectInfluence.lookup(sVertexName).isEmpty()) { indirectInfluence
		 * = indirectInfluence.union(sc.parallelizePairs( Arrays.asList(new
		 * Tuple2<String, List<String>>(sVertexName,
		 * OverThresholdVertex))));//accOverThresholdVertex.value())))); } }
		 * else { indirectInfluence = sc.parallelizePairs( Arrays.asList(new
		 * Tuple2<String, List<String>>(sVertexName,
		 * OverThresholdVertex)));//accOverThresholdVertex.value()))); } return
		 * fIndirectInfluence;//accBD.value();
		 */

		final Broadcast<List<Vertex>> bcVertices = sc.broadcast(vertices);
		final Broadcast<List<Edge>> bcEdges = sc.broadcast(edges);
		final Broadcast<String> bcVertexName = sc.broadcast(sVertexName);

		// rddVertices.cache();

		JavaPairRDD<String, BigDecimal> rddIndrInfl = rddVertices
				.mapToPair(vertex -> {
					String vName = vertex.getName();
					if (!vName.equals(bcVertexName.value())) {
						BigDecimal bd = IndirectInfluenceOfVertexOnOtherVertex(
								bcVertices.value(), bcEdges.value(),
								bcVertexName.value(), vName);
						return new Tuple2<String, BigDecimal>(vName, bd);
					} else {
						System.out
								.println("Suc anh huong gian tiep den chinh dinh do la 0.");
						return new Tuple2<String, BigDecimal>(vName,
								BigDecimal.ZERO);
					}
				});
		rddIndrInfl.cache();

		fIndirectInfluence = rddIndrInfl.values().reduce(
				(bd1, bd2) -> bd1.add(bd2));

		final Broadcast<BigDecimal> bcTheta = sc.broadcast(Data.theta);

		/*
		 * JavaPairRDD<String,List<String>> OverThresholdVertex =
		 * rddIndrInfl.filter(tuple -> { return
		 * (tuple._2.compareTo(bcTheta.value()) != -1); }).mapToPair(pairSB ->{
		 * return new Tuple2<String, List<String>>(bcVertexName.value(), new
		 * ArrayList<String>(Arrays.asList(pairSB._1))); }).reduceByKey((l1, l2)
		 * -> { l1.addAll(l2); return l1; });
		 */

		JavaPairRDD<String, List<String>> OverThresholdVertex = sc
				.parallelizePairs(Arrays
						.asList(new Tuple2<String, List<String>>(sVertexName,
								rddIndrInfl
										.filter(tuple -> {
											return (tuple._2.compareTo(bcTheta
													.value()) != -1);
										}).keys().collect())));
		if (indirectInfluence != null) {
			// if (indirectInfluence.lookup(sVertexName).isEmpty()) {
			indirectInfluence = indirectInfluence.union(OverThresholdVertex);
			// }
		} else {
			indirectInfluence = OverThresholdVertex;
			indirectInfluence.cache();
		}
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

	public List<String> findSmallestGroup(List<Vertex> vertices,
			JavaRDD<Vertex> rddVertices, List<Edge> edges) {
		int[] inputArray = new int[vertices.size()];
		List<int[]> subsets = new ArrayList<>();
		List<String> strSetS = new ArrayList<>();
		String[] arrayVertex = new String[vertices.size() + 1];
		for (int i = 1; i <= vertices.size(); i++) {
			inputArray[i - 1] = i;
		}
		for (int k = 1; k <= vertices.size(); k++) {
			subsets = findCombinations(vertices, inputArray, k);

			if (subsets.size() == vertices.size()) {
				for (int[] strings : subsets) {
					m_countChecking++;
					strSetS = new ArrayList<>();
					// System.out.println("a" + Arrays.toString(strings));
					for (int jj = 0; jj < strings.length; jj++) {
						strSetS.add(String.valueOf(strings[jj]));
					}
					System.out.println("***Xet tap: " + strSetS);
					for (String stt : strSetS) {
						InfluenceOfSetVertexOnAllVertex(vertices, rddVertices,
								edges, strSetS, stt);
						System.out.println("Dinh " + stt
								+ " anh huong toi cac dinh: "
								+ this.mapInfluented.get(stt));
					}
					int count = CalculateInfluenceOfSet(strSetS);
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
				this.mapInfluentedFirst = sortByComparator(
						this.mapInfluentedFirst, DESC);
				arrayVertex = addVertexAfterSorted(this.mapInfluentedFirst);
			} else {
				for (int[] strings : subsets) {
					m_countChecking++;
					strSetS = new ArrayList<>();
					// System.out.println("a" + Arrays.toString(strings));
					for (int jj = 0; jj < strings.length; jj++) {
						strSetS.add(String.valueOf(arrayVertex[strings[jj]]));
					}
					System.out.println("***Xet tap: " + strSetS);
					for (String stt : strSetS) {
						if (strSetS.size() > 1
								&& checkContain2List(
										this.mapInfluentedFirst.get(stt),
										strSetS) == true) {
							this.mapInfluented.put(stt,
									this.mapInfluentedFirst.get(stt));
							this.mapResult.put(stt,
									this.mapResultFirst.get(stt));
						} else {
							// System.out.println(stt);
							InfluenceOfSetVertexOnAllVertex(vertices,
									rddVertices, edges, strSetS, stt);
						}
						System.out.println("Dinh " + stt
								+ " anh huong toi cac dinh: "
								+ this.mapInfluented.get(stt));
					}
					int count = CalculateInfluenceOfSet(strSetS);
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
			}
		}
		System.out.println("So to hop phai duyet: " + m_countChecking);
		return new ArrayList<String>();
	}

	public JavaPairRDD<List<String>, BigDecimal> findSetKeyPlayers(
			List<Vertex> vertices, JavaRDD<Vertex> rddVertices, List<Edge> edges) {
		int[] inputArray = new int[vertices.size()];
		List<int[]> subsets = new ArrayList<>();
		List<String> strSetS = new ArrayList<>();
		List<Tuple2<BigDecimal, List<String>>> mUnsortedAll = new ArrayList<Tuple2<BigDecimal, List<String>>>(
				vertices.size());
		for (int i = 1; i <= vertices.size(); i++) {
			inputArray[i - 1] = i;
		}
		// Check influence of every vertex
		subsets = findCombinations(vertices, inputArray, 1);
		for (int[] strings : subsets) {
			strSetS = new ArrayList<>();
			for (int jj = 0; jj < strings.length; jj++) {
				strSetS.add(String.valueOf(strings[jj]));
			}
			for (String stt : strSetS) {
				System.out.println("\n***Xet dinh:" + stt);
				InfluenceOfSetVertexOnAllVertex(vertices, rddVertices, edges,
						strSetS, stt);
				System.out.println("Dinh " + stt + " anh huong toi cac dinh: "
						+ this.mapInfluented.get(stt));
			}
		}
		// check every set k vertexes
		subsets = findCombinations(vertices, inputArray, Data.iNeed);
		for (int[] strings : subsets) {
			strSetS = new ArrayList<>();
			// System.out.println("a" + Arrays.toString(strings));
			for (int jj = 0; jj < strings.length; jj++) {
				strSetS.add(String.valueOf(strings[jj]));
			}
			System.out.println("\n***Xet tap: " + strSetS);
			for (String stt : strSetS) {
				if (strSetS.size() > 1
						&& checkContain2List(this.mapInfluentedFirst.get(stt),
								strSetS) == true) {
					this.mapInfluented.put(stt,
							this.mapInfluentedFirst.get(stt));
					this.mapResult.put(stt, this.mapResultFirst.get(stt));
				} else {
					// System.out.println(stt);
					InfluenceOfSetVertexOnAllVertex(vertices, rddVertices,
							edges, strSetS, stt);
				}
				System.out.println("Dinh " + stt + " anh huong toi cac dinh: "
						+ this.mapInfluented.get(stt));
			}

			rddVertices.cache();

			mUnsortedAll.add(new Tuple2<BigDecimal, List<String>>(
					CalculateInfluenceOfSetKeyPlayers(strSetS), strSetS));
		}
		return sc.parallelizePairs(mUnsortedAll).sortByKey(false)
				.mapToPair(t -> t.swap());
	}

	private static List<String> union2List(List<String> list1,
			List<String> list2) {
		List<String> set = new ArrayList<>();

		set.addAll(list1);
		set.addAll(list2);

		return new ArrayList<String>(set);
	}

	private static List<String> union2ListRemove(List<String> list1,
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
			strSetUnionRemove = union2ListRemove(strSetUnion,
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
					if (!mapResult.isEmpty()) {
						if (mapResult.get(str).get(stt).compareTo(Data.theta) == 1) {
							System.out.println(stt
									+ ": "
									+ String.valueOf(mapResult.get(str)
											.get(stt)) + " --> Vuot nguong "
									+ Data.theta);
							countOk++;
						} else if (mapResult.get(str).get(stt)
								.compareTo(Data.theta) == -1
								&& mapResult.get(str).get(stt)
										.compareTo(BigDecimal.ZERO) == 1) {
							System.out.println(stt
									+ ": "
									+ String.valueOf(mapResult.get(str)
											.get(stt))
									+ " --> Khong Vuot nguong " + Data.theta);
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
										.subtract(mapResult.get(str).get(stt));
							} else {
								influentedValue = influentedValue
										.multiply(BigDecimal.ONE
												.subtract(mapResult.get(str)
														.get(stt)));
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
			strSetUnionRemove = union2ListRemove(strSetUnion,
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
}
