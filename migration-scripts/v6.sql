create table "pipeline_dependencies" ("pipeline_id" INTEGER NOT NULL,"dependent_pipeline_id" INTEGER NOT NULL);

alter table "pipeline_dependencies" add constraint "pipeline_dependencies_pipelines_dependency_fk" foreign key("dependent_pipeline_id") references "pipelines"("id") on update RESTRICT on delete CASCADE;
alter table "pipeline_dependencies" add constraint "pipeline_dependencies_pipelines_fk" foreign key("pipeline_id") references "pipelines"("id") on update RESTRICT on delete CASCADE;

create table "pipeline_progresses" ("pipeline_id" INTEGER NOT NULL,"progress" VARCHAR(40) NOT NULL);

alter table "pipeline_progresses" add constraint "pipeline_progresses_pk" primary key("pipeline_id");
alter table "pipeline_progresses" add constraint "pipeline_progresses_pipelines_fk" foreign key("pipeline_id") references "pipelines"("id") on update RESTRICT on delete CASCADE;