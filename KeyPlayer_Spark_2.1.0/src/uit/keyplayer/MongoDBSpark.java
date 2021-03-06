package uit.keyplayer;

import static com.mongodb.client.model.Filters.eq;

import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.List;

import org.apache.spark.api.java.JavaPairRDD;
import org.bson.Document;

import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import com.mongodb.MongoClient;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoCursor;
import com.mongodb.client.MongoDatabase;

import scala.Tuple2;

public class MongoDBSpark {
	private MongoClient mongoClient;

	private void MongoDBConnect() {
		mongoClient = new MongoClient("localhost", 27017);
	}

	private void MongoDBClose() {
		if (mongoClient != null) {
			mongoClient.close();
		} else {
			System.out.println("Khong the dong ket noi vi khong ton tai!");
		}
	}

	private MongoDatabase getDatabase() {
		MongoDBConnect();
		return this.mongoClient.getDatabase("keyplayer");
	}

	private MongoCollection<Document> getCollection() {
		MongoDatabase db = this.getDatabase();
		return db.getCollection("segment");
	}

	public void insertSegmentToMongoDB(List<Segment> segments) {
		MongoCollection<Document> col = this.getCollection();
		List<Document> listDoc = new ArrayList<Document>();
		for (Segment seg : segments) {
			Document doc = new Document("s", seg.getStartVertex()).append("e", seg.getEndVertex()).append("i",
					seg.getIndirectInfluence().toPlainString());
			//col.insertOne(doc);
			listDoc.add(doc);
		}
		col.insertMany(listDoc);
		MongoDBClose();
	}

	public List<Tuple2<String, BigDecimal>> getVertexSegment(String sVertexName) {
		List<Tuple2<String, BigDecimal>> allVertexPath = new ArrayList<Tuple2<String, BigDecimal>>();
		MongoCursor<Document> cur = this.getCollection().find(eq("s", sVertexName)).iterator();
		try {
			JsonParser parser = new JsonParser();
			while (cur.hasNext()) {
				JsonElement element = parser.parse(cur.next().toJson());
				JsonObject joPath = element.getAsJsonObject();
				allVertexPath.add(new Tuple2<String, BigDecimal>(joPath.get("e").getAsString(),
						joPath.get("i").getAsBigDecimal()));
			}
		} finally {
			cur.close();
			MongoDBClose();
		}
		return allVertexPath;
	}
}