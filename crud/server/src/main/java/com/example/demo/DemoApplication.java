package com.example.demo;

import io.opentelemetry.api.GlobalOpenTelemetry;
import io.opentelemetry.api.trace.Span;
import io.opentelemetry.api.trace.SpanKind;
import io.opentelemetry.api.trace.Tracer;
import io.opentelemetry.context.Context;
import io.opentelemetry.context.Scope;
import io.opentelemetry.context.propagation.TextMapGetter;
import io.r2dbc.spi.ConnectionFactory;
import lombok.*;
import lombok.extern.slf4j.Slf4j;
import org.springframework.boot.ApplicationArguments;
import org.springframework.boot.ApplicationRunner;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.Bean;
import org.springframework.core.io.ClassPathResource;
import org.springframework.data.annotation.Id;
import org.springframework.data.r2dbc.connectionfactory.init.CompositeDatabasePopulator;
import org.springframework.data.r2dbc.connectionfactory.init.ConnectionFactoryInitializer;
import org.springframework.data.r2dbc.connectionfactory.init.ResourceDatabasePopulator;
import org.springframework.data.r2dbc.repository.Query;
import org.springframework.data.r2dbc.repository.R2dbcRepository;
import org.springframework.data.relational.core.mapping.Column;
import org.springframework.data.relational.core.mapping.Table;
import org.springframework.messaging.handler.annotation.DestinationVariable;
import org.springframework.messaging.handler.annotation.MessageMapping;
import org.springframework.messaging.handler.annotation.Payload;
import org.springframework.stereotype.Component;
import org.springframework.stereotype.Controller;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

@SpringBootApplication
public class DemoApplication {

    public static void main(String[] args) {
        SpringApplication.run(DemoApplication.class, args);
    }

    @Bean
    public ConnectionFactoryInitializer initializer(ConnectionFactory connectionFactory) {

        ConnectionFactoryInitializer initializer = new ConnectionFactoryInitializer();
        initializer.setConnectionFactory(connectionFactory);

        CompositeDatabasePopulator populator = new CompositeDatabasePopulator();
        populator.addPopulators(new ResourceDatabasePopulator(new ClassPathResource("schema.sql")));
        populator.addPopulators(new ResourceDatabasePopulator(new ClassPathResource("data.sql")));
        initializer.setDatabasePopulator(populator);

        return initializer;
    }
}

@Component
@Slf4j
@RequiredArgsConstructor
class DataInitializer implements ApplicationRunner {

    private final PostRepository posts;

    @Override
    public void run(ApplicationArguments args) throws Exception {
        log.info("start data initialization...");
        this.posts
                .saveAll(
                        List.of(
                                Post.builder().title("Post one").content("The content of post one").build(),
                                Post.builder().title("Post tow").content("The content of post tow").build()
                        )
                )
                .log()
                .thenMany(
                        this.posts.findAll()
                )
                .subscribe(
                        (data) -> log.info("post: " + data),
                        (err) -> log.error("error: " + err),
                        () -> log.info("initialization done...")
                );
    }
}

@Controller
@RequiredArgsConstructor
class PostController {

    private static final Tracer TRACER =
            GlobalOpenTelemetry.getTracer("custom-rsocket-instrumentation");

    private static final TextMapGetter<Map<String, String>> GETTER = new MapTextMapGetter();

    private final PostRepository posts;

    @MessageMapping("posts.findAll")
    public Flux<Post> all(@Payload HashMap<String, String> tracecontext) {

        System.out.println("Trace context: " + tracecontext.toString());

        Context extractedContext = GlobalOpenTelemetry.get()
                .getPropagators()
                .getTextMapPropagator()
                .extract(Context.current(), tracecontext, GETTER);

        try (Scope scope = extractedContext.makeCurrent()) {
            Span span = TRACER
                    .spanBuilder("server-posts-findAll")
                    .setSpanKind(SpanKind.SERVER)
                    .setAttribute("service.name", "PostController")
                    .startSpan();

            var posts = this.posts.findAll();

            span.end();
            return posts;
        }
    }

    @MessageMapping("posts.titleContains")
    public Flux<Post> titleContains(@Payload String title) {
        return this.posts.findByTitleContains(title);
    }

    @MessageMapping("posts.findById.{id}")
    public Mono<Post> get(@DestinationVariable("id") Integer id) {
        return this.posts.findById(id);
    }

    @MessageMapping("posts.save")
    public Mono<Post> create(@Payload Post post) {
        return this.posts.save(post);
    }

    @MessageMapping("posts.update.{id}")
    public Mono<Post> update(@DestinationVariable("id") Integer id, @Payload Post post) {
        return this.posts.findById(id)
                .map(p -> {
                    p.setTitle(post.getTitle());
                    p.setContent(post.getContent());

                    return p;
                })
                .flatMap(p -> this.posts.save(p));
    }

    @MessageMapping("posts.deleteById.{id}")
    public Mono<Void> delete(@DestinationVariable("id") Integer id) {
        return this.posts.deleteById(id);
    }

}

interface PostRepository extends R2dbcRepository<Post, Integer> {

    @Query("SELECT * FROM posts WHERE title like $1")
    Flux<Post> findByTitleContains(String name);

}

@Data
@ToString
@Builder
@NoArgsConstructor
@AllArgsConstructor
@Table("posts")
class Post {

    @Id
    @Column("id")
    private Integer id;

    @Column("title")
    private String title;

    @Column("content")
    private String content;

}

class MapTextMapGetter implements TextMapGetter<Map<String, String>> {

    @Override
    public String get(Map<String, String> carrier, String key) {
        if (carrier != null) {
            return carrier.get(key);
        }
        return null;
    }

    @Override
    public Iterable<String> keys(Map<String, String> carrier) {
        return carrier.keySet();
    }
}