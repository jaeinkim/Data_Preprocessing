import java.io.BufferedWriter;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.nio.charset.Charset;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.*;



public class Main {
    public static List<List<String>> data;
    public static List<String> header;

    public static void main(String argv[]){
        String workDir = "D:\\jaein_kim\\GCP 활용 Data Lake 내부 프로젝트\\테스트 데이터\\스키마 수정 후\\data_preprocessing_csv\\";
        String[] fileNm = {
                "m_test1.csv"
                ,"m_test2.csv"
                ,"m_test3.csv"
                ,"m_test4.csv"
                ,"m_test5.csv"
                ,"m_test6.csv"
                ,"m_test7.csv"
                ,"m_test8.csv"
                ,"m_test9.csv"
                ,"m_test10.csv"
        };
        for (int i = 0; i < fileNm.length; i++ ){
            readCsv(workDir+fileNm[i]);

            changeDt(findColIdx("PQ_Fst_Sent_to_Clt_Dt"));
            changeDt(findColIdx("PO_Sent_to_Ven_Dt"));
            changeDt(findColIdx("Sch_Dlvr_Dt"));
            changeDt(findColIdx("Dlvrd_to_Clt_Dt"));
            changeDt(findColIdx("Dlvr_Rcded_Dt"));

            changeDouble(findColIdx("Weight_KG"));
            changeDouble(findColIdx("Freight_Cost_USD"));

            changeNull(findColIdx("Ln_Ite_Isrc_USD"));

//            deleteCol(findColIdx("Weight_KG"));
//            deleteCol(findColIdx("Dosage"));
//            deleteCol(findColIdx("Freight_Cost_USD"));
            writeCsv(workDir+"preprocessed_"+fileNm[i]);
        }


    }

    public static void changeNull(int idx){
        ArrayList<String> al_row = new ArrayList<String>();
        for(int i =0 ; i <data.size(); i++){
            try{
                if(data.get(i).get(idx) == null){
                    data.get(i).set(idx, "0");
                }
            }catch(Exception e){
                for(int j =0; j < data.get(i).size(); j++){
                    al_row.add(String.valueOf(data.get(i).get(j)));
                }
                al_row.add("0");
                String[] tmpList = al_row.toArray(new String[al_row.size()]);

                data.set(i, Arrays.asList(tmpList));
                al_row.clear();

            }
        }

    }
    public static void writeCsv(String path){
        BufferedWriter bufWriter = null;
        try{
            bufWriter = Files.newBufferedWriter(Paths.get(path), Charset.forName("UTF-8"));

//            for(String data: header){
            for(int i = 0; i < header.size(); i++){
                bufWriter.write(header.get(i));
                if(i == header.size() - 1){
                    continue;
                }
                bufWriter.write(",");
            }
            bufWriter.newLine();
            for(List<String> newLine : data){
                List<String> list = newLine;
                for(String data : list){
                    bufWriter.write(data);
                    bufWriter.write(",");
                }
//                //추가하기
//                bufWriter.write("주소");
                //개행코드추가
                bufWriter.newLine();
            }
        }catch(FileNotFoundException e){
            e.printStackTrace();
        }catch(IOException e){
            e.printStackTrace();
        }finally{
            try{
                if(bufWriter != null){
                    bufWriter.close();
                }
            }catch(IOException e){
//                e.printStackTrace();
            }
        }
    }
    public static void deleteCol(int idx){
        String[] tmp_arr;
        ArrayList<String> row_al = new ArrayList();
        ArrayList<String> header_al = new ArrayList();

//        System.out.println("idx = " + idx);
//        System.out.println("data.get(0).size() = " + data.get(0).size());
//
//        System.out.println("data.size() = " + data.size());

        for(int j=0; j < data.size(); j++){
            for(int i =0; i < data.get(j).size(); i++){
                row_al.add(String.valueOf(data.get(j).get(i)));
            }
            row_al.remove(idx);
//            System.out.println(row_al);
            String[] tmpList = row_al.toArray(new String[row_al.size()]);

            data.set(j, Arrays.asList(tmpList));
            row_al.clear();
        }
        for(int k =0; k < header.size(); k++){
            header_al.add(String.valueOf(header.get(k)));
        }
        header_al.remove(idx);
        header = header_al;
        header_al = null;
    }
    public static int findColIdx(String str){
        for (int i = 0; i < header.size(); i++){
            if(str.equals(header.get(i))){
                return i;
            }
        }
        return -1;
    }

    public static void readCsv(String path){
        data = new ArrayList<List<String>>();
        BufferedReader br = null;
        try {
            br = Files.newBufferedReader(Paths.get(path));
            String line = "";
            boolean head_flg = true;
            while((line = br.readLine()) != null){
                List<String> tmpList = new ArrayList<String>();
                /***
                 *  Issue: 데이터 중 아래와 같은 것이 있으면 한 컬럼의 데이터가 여러 컬럼으로 분해됨
                 *  "AAA","Lamivudine 10mg/ml, oral solution w/syringe, Bottle, 240 ml","BBB"
                 */
//                String[] array = line.split(",");
                String[] array = line.split(",(?=(?:[^\"]*\"[^\"]*\")*(?![^\"]*\"))");

                if(head_flg == true){
                    head_flg = false;
                    tmpList = Arrays.asList(array);
                    header = tmpList;
                    continue;
                }
                tmpList = Arrays.asList(array);
//                System.out.println(tmpList);
                data.add(tmpList);
            }
        } catch (IOException e) {
            e.printStackTrace();
        }finally{
            try {
                br.close();
            } catch (IOException e) {
                e.printStackTrace();
            }

        }
    }
    public static void changeDouble(int idx){
        for(List row : data){
            try {

                Double.valueOf((String) row.get(idx));
            }catch(Exception e) {
                System.out.println("(String) row.get(idx) = " + (String) row.get(idx));
                row.set(idx, "0");

            }
        }
    }

    /***
     * function: abnormal data -> normal date
     * Targeted date format: yyyy-MM-dd
     * abnormal data: 11/30/11, 'Pre-PQ Process' etc
     *   String type is changed into '9999-12-31'.
     * @param
     * @return
     */
    public static void changeDt(int idx){
        for(List row : data){
            String from = String.valueOf(row.get(idx)).trim();

            System.out.println("from = " + from);

            SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd");
            SimpleDateFormat sdf2 = new SimpleDateFormat("MM/dd/yy");
//            SimpleDateFormat sdf3 = new SimpleDateFormat("dd-MMM-yy"); // 2019-06-22 -> 22-6월-19
            SimpleDateFormat sdf3 = new SimpleDateFormat("dd-MMM-yy", Locale.ENGLISH); // 2019-06-22 -> 22-6월-19
            sdf3.setLenient(false);

//            String test_dt = "22-Jul-19";
////            String test_dt = "2019-06-22";
//
//            try {
//                Date test_dt2 = sdf3.parse(test_dt);
//                String test2 = sdf.format(test_dt2);
//                System.out.println("!!! test2 = " + test2);
//            } catch (ParseException e) {
//                System.out.println("error 발생");
//                e.printStackTrace();
//            }

            Date to = null;
            try {
                to = sdf.parse(from);
                String ret  = sdf.format(to);
                row.set(idx, ret); //문제
                continue;
            } catch (ParseException e) {
//                e.printStackTrace();
            }

            try{
                to = sdf2.parse(from);
                String ret  = sdf.format(to);
                row.set(idx, ret); //문제
                continue;
            } catch (ParseException e) {
//                System.out.println("3. row.get(idx) = " + row.get(idx));
//                e.printStackTrace();

            }
            try {
                to = sdf3.parse(from);
                System.out.println("here???");
                String ret  = sdf.format(to);
                row.set(idx, ret); //문제

            } catch (ParseException e) {
                e.printStackTrace();
                System.out.println("3. row.get(idx) = " + row.get(idx));
                row.set(idx, "9999-12-31");
            }
        }
    }
}

