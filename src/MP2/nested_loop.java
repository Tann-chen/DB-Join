import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;


public class nested_loop {

    static int outer_IO = 0;
    static int inner_IO = 0;


    public static void Block_based_nested_loop() throws IOException {

        File file1 = new File("./src/Project2/JoinT1.txt"); // small relation table
        File file2 = new File("./src/Project2/JoinT2.txt"); // large relation table
        File file3 = new File("./src/Project2/JoinT3.txt"); // output file


        FileInputStream fileInputStream1 = new FileInputStream(file1);
//        FileInputStream fileInputStream2 = new FileInputStream(file2);
        FileOutputStream fileOutputStream = new FileOutputStream(file3);

        byte[] block1 = new byte[4040 * 1000];
        byte[] block2 = new byte[3640];


        int a;
        while ((a = fileInputStream1.read(block1)) != -1) {

            outer_IO++;

            FileInputStream fileInputStream2 = new FileInputStream(file2);

            ArrayList<Float> grade = new ArrayList<>();

            Map<String, ArrayList<Float>> studentCourse = new HashMap<>();

            byte[] studentIDBYTES = new byte[8];
            String studentID = null;

            for (int i = 0; i < a; i += 101) {
                for (int j = i, k = 0; j < i + 8; j++, k++) {
                    studentIDBYTES[k] = block1[j];
                }
                studentID = new String(studentIDBYTES);

                studentCourse.put(studentID, grade);
            }

            block1 = new byte[4040 * 1000];

            int b;
            while ((b = fileInputStream2.read(block2)) != -1) {

                inner_IO++;

                byte[] enrollment_studentIDBYTES = new byte[8];
                String enrollment_studentID = null;

                byte[] enrollment_creditBYTES = new byte[2];
                String enrollment_credit = null;

                byte[] enrollment_gradeBYTES = new byte[4];
                String enrollment_grade = null;

                for (int i = 0; i < block2.length; i += 28) {
                    for (int j = i, k = 0; j < i + 8; j++, k++) {
                        enrollment_studentIDBYTES[k] = block2[j]; // get studentID
                    }
                    enrollment_studentID = new String(enrollment_studentIDBYTES);

                    for (int j = i + 21, k = 0; j < i + 21 + 2; j++, k++) {
                        enrollment_creditBYTES[k] = block2[j]; // get credits
                    }
                    enrollment_credit = new String(enrollment_creditBYTES);

                    for (int j = i + 23, k = 0; j < i + 23 + 4; j++, k++) {
                        enrollment_gradeBYTES[k] = block2[j]; // get grade
                    }

                    enrollment_grade = new String(enrollment_gradeBYTES);

//                    System.out.println("Enrollment (Student ID: "+enrollment_studentID+" ,Credits: "+enrollment_credit+" ,Grade: "+enrollment_grade+")");

                    float credit = Float.parseFloat(enrollment_credit);

                    float grade_number = Grade.castGradeToVal(enrollment_grade.trim());

                    float accumulatedGrade = Float.parseFloat(enrollment_credit) * grade_number;

                    if (studentCourse.containsKey(enrollment_studentID)) {

                        if (studentCourse.get(enrollment_studentID) == null || studentCourse.get(enrollment_studentID).size() == 0) {
                            grade = new ArrayList<>();

                            grade.add(credit);

                            grade.add(accumulatedGrade);

                        } else {

                            grade = studentCourse.get(enrollment_studentID);

                            float update_credit = grade.get(0) + credit;

                            float update_grade = grade.get(1) + accumulatedGrade;

                            grade.set(0, update_credit);

                            grade.set(1, update_grade);

                        }
                        studentCourse.put(enrollment_studentID, grade);
                    }

                }
            }

            String output = null;

            Iterator<Map.Entry<String, ArrayList<Float>>> entryIterator = studentCourse.entrySet().iterator();

            while (entryIterator.hasNext()) {
                Map.Entry<String, ArrayList<Float>> next = entryIterator.next();
                String key = next.getKey();

                output = key + " ";

                ArrayList<Float> value = next.getValue();
                if (value == null || value.size() == 0) {

                    output += "0";

                } else {
                    Float all_credit = value.get(0);
                    Float all_grade = value.get(1);
                    Float average_grade = all_grade / all_credit;

                    output += String.valueOf(average_grade);

                }

                output += "\n";

                fileOutputStream.write(output.getBytes());

            }

            studentCourse.clear();// empty hashmap
            fileInputStream2.close();

        }

        fileInputStream1.close();

        fileOutputStream.flush();
        fileOutputStream.close();

        System.out.println("Outer I/O times: " + outer_IO);
        System.out.println("Inner I/O times: " + inner_IO);

    }


    public static void main(String[] args) throws IOException {

        long startTime = System.currentTimeMillis();

        Block_based_nested_loop();

        long endTime = System.currentTimeMillis();

        System.out.println("Running Time: " + (endTime - startTime) / 1000 + " s");

    }
}
