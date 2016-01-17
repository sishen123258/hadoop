//package MapReduce;
//
//import domain.NcdcRecordParser;
//import org.apache.hadoop.io.IntWritable;
//import org.apache.hadoop.io.LongWritable;
//import org.apache.hadoop.io.Text;
//import org.apache.hadoop.mapreduce.Mapper;
//
//import java.io.IOException;
//
///**
// * Created by root on 12/16/15.
// */
//public class MaxTemperatureMapper extends Mapper<LongWritable,Text,Text,IntWritable> {
//
//    private NcdcRecordParser parser=new NcdcRecordParser();
//
//    @Override
//    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
////        String line=value.toString();
////        String year=line.substring(15,19);
////        int airTem=Integer.parseInt(line.substring(87,92));
////        context.write(new Text(year),new IntWritable(airTem));
//
//
//        parser.parse(value);
//
//        if(parser.isValidTemperature()){
//            context.write(new Text(parser.getYear()),new IntWritable(parser.getAirTemperature()));
//        }
//
//    }
//
//}
