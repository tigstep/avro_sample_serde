import java.io.File;
import java.io.IOException;

import org.apache.avro.file.DataFileWriter;
import org.apache.avro.io.DatumWriter;
import org.apache.avro.specific.SpecificDatumWriter;
import org.tigstep.person.person;

public class AvroSerializer {
    public static void main(String[] args) throws IOException {
        person psn1 = new person();
        psn1.setFirstName("John");
        psn1.setLastName("Doe");
        psn1.setSex("male");
        // Serialize emp1 to disk
        File file = new File("employees_serialized.avro");
        DatumWriter<person> userDatumWriter = new
                SpecificDatumWriter<person>(person.class);
        DataFileWriter<person> dataFileWriter = new
                DataFileWriter<person>(userDatumWriter);

        dataFileWriter.create(psn1.getSchema(), file);

        dataFileWriter.append(psn1);
        dataFileWriter.close();
    }
}