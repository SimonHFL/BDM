package project2;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import org.codehaus.jettison.json.JSONException;
import org.codehaus.jettison.json.JSONObject;
import org.codehaus.jettison.json.JSONArray;


public class FirstMapper extends Mapper<LongWritable, Text, LongWritable, Text> {

    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

        String line = value.toString();

        try{
            JSONArray array = new JSONArray(line);

            for(int i=0;i<array.length(); i++){

                JSONObject obj = array.getJSONObject(i);
                JSONObject result = new JSONObject();

                result.put("uuid", obj.getString("uuid"));
                result.put("Location", obj.getJSONObject("Metadata").getJSONObject("Location"));

                context.write(key, new Text(result.toString())); // send key and value to reducer
            }
        }catch(JSONException e){
            e.printStackTrace();
        }
    }

}