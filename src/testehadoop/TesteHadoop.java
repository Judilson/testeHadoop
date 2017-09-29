/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package testehadoop;

import br.com.gex.TesteConexao;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

/**
 *
 * @author jjunior
 */
public class TesteHadoop extends Configured implements Tool {

    /**
     * @param args the command line arguments
     */
    public static void main(String[] args) throws Exception {

        System.exit(ToolRunner.run(new TesteHadoop(),args));

    }

    @Override
    public int run(String[] strings) throws Exception {
        new TesteConexao().teste();
        throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.

    }

}
