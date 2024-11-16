package com.alura.codetickets;

import org.springframework.batch.core.Job;
import org.springframework.batch.core.Step;
import org.springframework.batch.core.job.builder.JobBuilder;
import org.springframework.batch.core.launch.support.RunIdIncrementer;
import org.springframework.batch.core.repository.JobRepository;
import org.springframework.batch.core.step.builder.StepBuilder;
import org.springframework.batch.core.step.tasklet.Tasklet;
import org.springframework.batch.item.ItemReader;
import org.springframework.batch.item.ItemWriter;
import org.springframework.batch.item.database.BeanPropertyItemSqlParameterSourceProvider;
import org.springframework.batch.item.database.builder.JdbcBatchItemWriterBuilder;
import org.springframework.batch.item.file.builder.FlatFileItemReaderBuilder;
import org.springframework.batch.repeat.RepeatStatus;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.io.FileSystemResource;
import org.springframework.transaction.PlatformTransactionManager;

import javax.sql.DataSource;
import java.io.File;

@Configuration
public class ImportacaoJobConfiguration {

    @Autowired
    private PlatformTransactionManager transactionManager;

    @Bean
    public Job job(Step passoInicial, JobRepository jobRepository) {
        return new JobBuilder("geracao-tickets", jobRepository)
                .start(passoInicial)
                .next(moverArquivosStep(jobRepository))
                .incrementer(new RunIdIncrementer())
                .build();
    }

    @Bean
    public Step passoInicial(ItemReader<Importacao> reader, ItemWriter<Importacao> writer, JobRepository jobRepository) {
        return new StepBuilder("passo-inicial", jobRepository)
                .<Importacao, Importacao>chunk(200, transactionManager)
                .reader(reader)
                .processor(processor())
                .writer(writer)
                .build();
    }

    @Bean
    public ItemReader<Importacao> reader() {
//        para csv(FlatFileItemReaderBuilder)
        return new FlatFileItemReaderBuilder<Importacao>()
                .name("leitura-csv")
                .resource(new FileSystemResource("files/dados.csv"))
                .comments("--")
                .delimited()
                .delimiter(";")
                .names("cpf", "cliente", "nascimento", "evento", "data", "tipoIngresso", "valor")
                .fieldSetMapper(new ImportacaoMapper())
                .build();
    }

    @Bean
    public ItemWriter<Importacao> writer(DataSource dataSource) {
        return new JdbcBatchItemWriterBuilder<Importacao>()
                .dataSource(dataSource)
                .sql(
                        "INSERT INTO importacao (cliente, cpf, data, evento, hora_importacao, nascimento, tipo_ingresso, valor, taxa_adm)\n" +
                                "VALUES(:cliente, :cpf, :data, :evento, :horaImportacao, :nascimento, :tipoIngresso, :valor, :taxaAdm);"
                )
                .itemSqlParameterSourceProvider(new BeanPropertyItemSqlParameterSourceProvider<>())
                .build();
    }

    @Bean
    public ImportacaoProcessor processor(){
        return new ImportacaoProcessor();
    }

    @Bean
    public Tasklet moverArquivosTasklet() {
        // Define e retorna um Tasklet, que é uma unidade de trabalho no Spring Batch
        return (contribution, chunkContext) -> {
            // Define a pasta de origem dos arquivos
            File pastaOrigem = new File("files");

            // Define a pasta de destino para onde os arquivos serão movidos
            File pastaDestino = new File("imported-files");

            // Verifica se a pasta de destino existe; se não existir, cria a pasta
            if (!pastaDestino.exists()) {
                pastaDestino.mkdirs(); // Cria a estrutura de diretórios se necessário
            }

            // Lista todos os arquivos na pasta de origem que possuem a extensão ".csv"
            File[] arquivos = pastaOrigem.listFiles((dir, name) -> name.endsWith(".csv"));

            // Verifica se existem arquivos para processar
            if (arquivos != null) {
                // Itera sobre cada arquivo encontrado
                for (File arquivo : arquivos) {
                    // Cria o caminho completo do arquivo de destino com o mesmo nome do arquivo original
                    File arquivoDestino = new File(pastaDestino, arquivo.getName());

                    // Tenta mover o arquivo da origem para o destino
                    if (arquivo.renameTo(arquivoDestino)) {
                        // Loga uma mensagem indicando que o arquivo foi movido com sucesso
                        System.out.println("Arquivo movido: " + arquivo.getName());
                    } else {
                        // Lança uma exceção caso o arquivo não possa ser movido
                        throw new RuntimeException("Não foi possível mover o arquivo: " + arquivo.getName());
                    }
                }
            }

            // Retorna o status do Tasklet como FINALIZADO, indicando que o trabalho foi concluído
            return RepeatStatus.FINISHED;
        };
    }

    @Bean
    public Step moverArquivosStep(JobRepository jobRepository) {
        return new StepBuilder("mover-arquivo", jobRepository)
                .tasklet(moverArquivosTasklet(), transactionManager)
                .build();
    }

}
