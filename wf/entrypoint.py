import os
import shutil
import subprocess
import typing
from pathlib import Path

import requests
import typing_extensions
from flytekit.core.annotation import FlyteAnnotation
from latch.ldata.path import LPath
from latch.resources.tasks import custom_task, nextflow_runtime_task
from latch.resources.workflow import workflow
from latch.types import metadata
from latch.types.directory import LatchDir
from latch_cli.nextflow.utils import _get_execution_name
from latch_cli.nextflow.workflow import get_flag
from latch_cli.services.register.utils import import_module_by_path
from latch_cli.utils import urljoins

meta = Path("latch_metadata") / "__init__.py"
import_module_by_path(meta)


@custom_task(cpu=0.25, memory=0.5, storage_gib=1)
def initialize() -> str:
    token = os.environ.get("FLYTE_INTERNAL_EXECUTION_ID")
    if token is None:
        raise RuntimeError("failed to get execution token")

    headers = {"Authorization": f"Latch-Execution-Token {token}"}

    print("Provisioning shared storage volume... ", end="")
    resp = requests.post(
        "http://nf-dispatcher-service.flyte.svc.cluster.local/provision-storage",
        headers=headers,
        json={
            "storage_gib": 100,
        },
    )
    resp.raise_for_status()
    print("Done.")

    return resp.json()["name"]


@nextflow_runtime_task(cpu=4, memory=8, storage_gib=100)
def nextflow_runtime(
    pvc_name: str,
    name: typing.Optional[str],
    single_end: typing.Optional[bool],
    outdir: typing.Optional[
        typing_extensions.Annotated[LatchDir, FlyteAnnotation({"output": True})]
    ],
    fasta_host: typing.Optional[str],
    fasta_pathogen: typing.Optional[str],
    gff_host_genome: typing.Optional[str],
    gff_host_tRNA: typing.Optional[str],
    gff_pathogen: typing.Optional[str],
    transcriptome_host: typing.Optional[str],
    transcriptome_pathogen: typing.Optional[str],
    read_transcriptome_fasta_host_from_file: typing.Optional[bool],
    read_transcriptome_fasta_pathogen_from_file: typing.Optional[bool],
    genomes_ignore: typing.Optional[bool],
    skip_fastqc: typing.Optional[bool],
    fastqc_params: typing.Optional[str],
    run_cutadapt: typing.Optional[bool],
    cutadapt_params: typing.Optional[str],
    run_bbduk: typing.Optional[bool],
    bbduk_params: typing.Optional[str],
    libtype: typing.Optional[str],
    incompatPrior: typing.Optional[int],
    generate_salmon_uniq_ambig: typing.Optional[bool],
    run_salmon_selective_alignment: typing.Optional[bool],
    writeUnmappedNames: typing.Optional[bool],
    softclipOverhangs: typing.Optional[bool],
    dumpEq: typing.Optional[bool],
    writeMappings: typing.Optional[bool],
    keepDuplicates: typing.Optional[bool],
    salmon_sa_params_index: typing.Optional[str],
    salmon_sa_params_mapping: typing.Optional[str],
    run_salmon_alignment_based_mode: typing.Optional[bool],
    salmon_alignment_based_params: typing.Optional[str],
    run_star: typing.Optional[bool],
    star_salmon_index_params: typing.Optional[str],
    star_salmon_alignment_params: typing.Optional[str],
    star_index_params: typing.Optional[str],
    star_alignment_params: typing.Optional[str],
    run_htseq_uniquely_mapped: typing.Optional[bool],
    htseq_params: typing.Optional[str],
    mapping_statistics: typing.Optional[bool],
    input: str,
    genome_host: typing.Optional[str],
    genome_pathogen: typing.Optional[str],
    a: typing.Optional[str],
    A: typing.Optional[str],
    quality_cutoff: typing.Optional[int],
    minlen: typing.Optional[int],
    qtrim: typing.Optional[str],
    trimq: typing.Optional[int],
    ktrim: typing.Optional[str],
    k: typing.Optional[int],
    mink: typing.Optional[int],
    hdist: typing.Optional[int],
    adapters: typing.Optional[str],
    gene_feature_gff_to_create_transcriptome_host: typing.Optional[str],
    gene_feature_gff_to_create_transcriptome_pathogen: typing.Optional[str],
    gene_attribute_gff_to_create_transcriptome_host: typing.Optional[str],
    gene_attribute_gff_to_create_transcriptome_pathogen: typing.Optional[str],
    kmer_length: typing.Optional[int],
    outSAMunmapped: typing.Optional[str],
    outSAMattributes: typing.Optional[str],
    outFilterMultimapNmax: typing.Optional[int],
    outFilterType: typing.Optional[str],
    alignSJoverhangMin: typing.Optional[int],
    alignSJDBoverhangMin: typing.Optional[int],
    outFilterMismatchNmax: typing.Optional[int],
    outFilterMismatchNoverReadLmax: typing.Optional[int],
    alignIntronMin: typing.Optional[int],
    alignIntronMax: typing.Optional[int],
    alignMatesGapMax: typing.Optional[int],
    limitBAMsortRAM: typing.Optional[int],
    winAnchorMultimapNmax: typing.Optional[int],
    sjdbOverhang: typing.Optional[int],
    quantTranscriptomeBan: typing.Optional[str],
    outWigType: typing.Optional[str],
    outWigStrand: typing.Optional[str],
    stranded: typing.Optional[str],
    max_reads_in_buffer: typing.Optional[int],
    minaqual: typing.Optional[int],
    gene_feature_gff_to_quantify_host: typing.Optional[str],
    host_gff_attribute: typing.Optional[str],
    gene_feature_gff_to_quantify_pathogen: typing.Optional[str],
    pathogen_gff_attribute: typing.Optional[str],
    rna_classes_to_replace_host: typing.Optional[str],
) -> None:
    try:
        shared_dir = Path("/nf-workdir")

        ignore_list = [
            "latch",
            ".latch",
            "nextflow",
            ".nextflow",
            "work",
            "results",
            "miniconda",
            "anaconda3",
            "mambaforge",
        ]

        shutil.copytree(
            Path("/root"),
            shared_dir,
            ignore=lambda src, names: ignore_list,
            ignore_dangling_symlinks=True,
            dirs_exist_ok=True,
        )

        cmd = [
            "/root/nextflow",
            "run",
            str(shared_dir / "main.nf"),
            "-work-dir",
            str(shared_dir),
            "-profile",
            "docker",
            "-c",
            "latch.config",
            *get_flag("name", name),
            *get_flag("input", input),
            *get_flag("single_end", single_end),
            *get_flag("outdir", outdir),
            *get_flag("fasta_host", fasta_host),
            *get_flag("fasta_pathogen", fasta_pathogen),
            *get_flag("gff_host_genome", gff_host_genome),
            *get_flag("gff_host_tRNA", gff_host_tRNA),
            *get_flag("gff_pathogen", gff_pathogen),
            *get_flag("transcriptome_host", transcriptome_host),
            *get_flag("transcriptome_pathogen", transcriptome_pathogen),
            *get_flag(
                "read_transcriptome_fasta_host_from_file",
                read_transcriptome_fasta_host_from_file,
            ),
            *get_flag(
                "read_transcriptome_fasta_pathogen_from_file",
                read_transcriptome_fasta_pathogen_from_file,
            ),
            *get_flag("genome_host", genome_host),
            *get_flag("genome_pathogen", genome_pathogen),
            *get_flag("genomes_ignore", genomes_ignore),
            *get_flag("skip_fastqc", skip_fastqc),
            *get_flag("fastqc_params", fastqc_params),
            *get_flag("run_cutadapt", run_cutadapt),
            *get_flag("a", a),
            *get_flag("A", A),
            *get_flag("quality_cutoff", quality_cutoff),
            *get_flag("cutadapt_params", cutadapt_params),
            *get_flag("run_bbduk", run_bbduk),
            *get_flag("minlen", minlen),
            *get_flag("qtrim", qtrim),
            *get_flag("trimq", trimq),
            *get_flag("ktrim", ktrim),
            *get_flag("k", k),
            *get_flag("mink", mink),
            *get_flag("hdist", hdist),
            *get_flag("adapters", adapters),
            *get_flag("bbduk_params", bbduk_params),
            *get_flag("libtype", libtype),
            *get_flag("incompatPrior", incompatPrior),
            *get_flag("generate_salmon_uniq_ambig", generate_salmon_uniq_ambig),
            *get_flag(
                "gene_feature_gff_to_create_transcriptome_host",
                gene_feature_gff_to_create_transcriptome_host,
            ),
            *get_flag(
                "gene_feature_gff_to_create_transcriptome_pathogen",
                gene_feature_gff_to_create_transcriptome_pathogen,
            ),
            *get_flag(
                "gene_attribute_gff_to_create_transcriptome_host",
                gene_attribute_gff_to_create_transcriptome_host,
            ),
            *get_flag(
                "gene_attribute_gff_to_create_transcriptome_pathogen",
                gene_attribute_gff_to_create_transcriptome_pathogen,
            ),
            *get_flag("run_salmon_selective_alignment", run_salmon_selective_alignment),
            *get_flag("kmer_length", kmer_length),
            *get_flag("writeUnmappedNames", writeUnmappedNames),
            *get_flag("softclipOverhangs", softclipOverhangs),
            *get_flag("dumpEq", dumpEq),
            *get_flag("writeMappings", writeMappings),
            *get_flag("keepDuplicates", keepDuplicates),
            *get_flag("salmon_sa_params_index", salmon_sa_params_index),
            *get_flag("salmon_sa_params_mapping", salmon_sa_params_mapping),
            *get_flag(
                "run_salmon_alignment_based_mode", run_salmon_alignment_based_mode
            ),
            *get_flag("salmon_alignment_based_params", salmon_alignment_based_params),
            *get_flag("run_star", run_star),
            *get_flag("outSAMunmapped", outSAMunmapped),
            *get_flag("outSAMattributes", outSAMattributes),
            *get_flag("outFilterMultimapNmax", outFilterMultimapNmax),
            *get_flag("outFilterType", outFilterType),
            *get_flag("alignSJoverhangMin", alignSJoverhangMin),
            *get_flag("alignSJDBoverhangMin", alignSJDBoverhangMin),
            *get_flag("outFilterMismatchNmax", outFilterMismatchNmax),
            *get_flag("outFilterMismatchNoverReadLmax", outFilterMismatchNoverReadLmax),
            *get_flag("alignIntronMin", alignIntronMin),
            *get_flag("alignIntronMax", alignIntronMax),
            *get_flag("alignMatesGapMax", alignMatesGapMax),
            *get_flag("limitBAMsortRAM", limitBAMsortRAM),
            *get_flag("winAnchorMultimapNmax", winAnchorMultimapNmax),
            *get_flag("sjdbOverhang", sjdbOverhang),
            *get_flag("quantTranscriptomeBan", quantTranscriptomeBan),
            *get_flag("star_salmon_index_params", star_salmon_index_params),
            *get_flag("star_salmon_alignment_params", star_salmon_alignment_params),
            *get_flag("outWigType", outWigType),
            *get_flag("outWigStrand", outWigStrand),
            *get_flag("star_index_params", star_index_params),
            *get_flag("star_alignment_params", star_alignment_params),
            *get_flag("run_htseq_uniquely_mapped", run_htseq_uniquely_mapped),
            *get_flag("stranded", stranded),
            *get_flag("max_reads_in_buffer", max_reads_in_buffer),
            *get_flag("minaqual", minaqual),
            *get_flag("htseq_params", htseq_params),
            *get_flag(
                "gene_feature_gff_to_quantify_host", gene_feature_gff_to_quantify_host
            ),
            *get_flag("host_gff_attribute", host_gff_attribute),
            *get_flag(
                "gene_feature_gff_to_quantify_pathogen",
                gene_feature_gff_to_quantify_pathogen,
            ),
            *get_flag("pathogen_gff_attribute", pathogen_gff_attribute),
            *get_flag("mapping_statistics", mapping_statistics),
            *get_flag("rna_classes_to_replace_host", rna_classes_to_replace_host),
        ]

        print("Launching Nextflow Runtime")
        print(" ".join(cmd))
        print(flush=True)

        env = {
            **os.environ,
            "NXF_HOME": "/root/.nextflow",
            "NXF_OPTS": "-Xms2048M -Xmx8G -XX:ActiveProcessorCount=4",
            "K8S_STORAGE_CLAIM_NAME": pvc_name,
            "NXF_DISABLE_CHECK_LATEST": "true",
        }
        subprocess.run(
            cmd,
            env=env,
            check=True,
            cwd=str(shared_dir),
        )
    finally:
        print()

        nextflow_log = shared_dir / ".nextflow.log"
        if nextflow_log.exists():
            name = _get_execution_name()
            if name is None:
                print("Skipping logs upload, failed to get execution name")
            else:
                remote = LPath(
                    urljoins(
                        "latch:///your_log_dir/nf_nf_core_dualrnaseq",
                        name,
                        "nextflow.log",
                    )
                )
                print(f"Uploading .nextflow.log to {remote.path}")
                remote.upload_from(nextflow_log)


@workflow(metadata._nextflow_metadata)
def nf_nf_core_dualrnaseq(
    name: typing.Optional[str],
    single_end: typing.Optional[bool],
    outdir: typing.Optional[
        typing_extensions.Annotated[LatchDir, FlyteAnnotation({"output": True})]
    ],
    fasta_host: typing.Optional[str],
    fasta_pathogen: typing.Optional[str],
    gff_host_genome: typing.Optional[str],
    gff_host_tRNA: typing.Optional[str],
    gff_pathogen: typing.Optional[str],
    transcriptome_host: typing.Optional[str],
    transcriptome_pathogen: typing.Optional[str],
    read_transcriptome_fasta_host_from_file: typing.Optional[bool],
    read_transcriptome_fasta_pathogen_from_file: typing.Optional[bool],
    genomes_ignore: typing.Optional[bool],
    skip_fastqc: typing.Optional[bool],
    fastqc_params: typing.Optional[str],
    run_cutadapt: typing.Optional[bool],
    cutadapt_params: typing.Optional[str],
    run_bbduk: typing.Optional[bool],
    bbduk_params: typing.Optional[str],
    libtype: typing.Optional[str],
    incompatPrior: typing.Optional[int],
    generate_salmon_uniq_ambig: typing.Optional[bool],
    run_salmon_selective_alignment: typing.Optional[bool],
    writeUnmappedNames: typing.Optional[bool],
    softclipOverhangs: typing.Optional[bool],
    dumpEq: typing.Optional[bool],
    writeMappings: typing.Optional[bool],
    keepDuplicates: typing.Optional[bool],
    salmon_sa_params_index: typing.Optional[str],
    salmon_sa_params_mapping: typing.Optional[str],
    run_salmon_alignment_based_mode: typing.Optional[bool],
    salmon_alignment_based_params: typing.Optional[str],
    run_star: typing.Optional[bool],
    star_salmon_index_params: typing.Optional[str],
    star_salmon_alignment_params: typing.Optional[str],
    star_index_params: typing.Optional[str],
    star_alignment_params: typing.Optional[str],
    run_htseq_uniquely_mapped: typing.Optional[bool],
    htseq_params: typing.Optional[str],
    mapping_statistics: typing.Optional[bool],
    input: str = "data/*{1,2}.fastq.gz",
    genome_host: typing.Optional[str] = "GRCh38",
    genome_pathogen: typing.Optional[str] = "SL1344",
    a: typing.Optional[str] = "AGATCGGAAGAGCACACGTCTGAACTCCAGTCA",
    A: typing.Optional[str] = "AGATCGGAAGAGCGTCGTGTAGGGAAAGAGTGT",
    quality_cutoff: typing.Optional[int] = 10,
    minlen: typing.Optional[int] = 18,
    qtrim: typing.Optional[str] = "r",
    trimq: typing.Optional[int] = 10,
    ktrim: typing.Optional[str] = "r",
    k: typing.Optional[int] = 17,
    mink: typing.Optional[int] = 11,
    hdist: typing.Optional[int] = 1,
    adapters: typing.Optional[str] = "data/adapters.fa",
    gene_feature_gff_to_create_transcriptome_host: typing.Optional[
        str
    ] = "['exon', 'tRNA']",
    gene_feature_gff_to_create_transcriptome_pathogen: typing.Optional[
        str
    ] = "['gene', 'sRNA', 'tRNA', 'rRNA']",
    gene_attribute_gff_to_create_transcriptome_host: typing.Optional[
        str
    ] = "transcript_id",
    gene_attribute_gff_to_create_transcriptome_pathogen: typing.Optional[
        str
    ] = "locus_tag",
    kmer_length: typing.Optional[int] = 21,
    outSAMunmapped: typing.Optional[str] = "Within",
    outSAMattributes: typing.Optional[str] = "Standard",
    outFilterMultimapNmax: typing.Optional[int] = 999,
    outFilterType: typing.Optional[str] = "BySJout",
    alignSJoverhangMin: typing.Optional[int] = 8,
    alignSJDBoverhangMin: typing.Optional[int] = 1,
    outFilterMismatchNmax: typing.Optional[int] = 999,
    outFilterMismatchNoverReadLmax: typing.Optional[int] = 1,
    alignIntronMin: typing.Optional[int] = 20,
    alignIntronMax: typing.Optional[int] = 1000000,
    alignMatesGapMax: typing.Optional[int] = 1000000,
    limitBAMsortRAM: typing.Optional[int] = 0,
    winAnchorMultimapNmax: typing.Optional[int] = 999,
    sjdbOverhang: typing.Optional[int] = 100,
    quantTranscriptomeBan: typing.Optional[str] = "Singleend",
    outWigType: typing.Optional[str] = "None",
    outWigStrand: typing.Optional[str] = "Stranded",
    stranded: typing.Optional[str] = "yes",
    max_reads_in_buffer: typing.Optional[int] = 30000000,
    minaqual: typing.Optional[int] = 10,
    gene_feature_gff_to_quantify_host: typing.Optional[str] = "['exon', 'tRNA']",
    host_gff_attribute: typing.Optional[str] = "gene_id",
    gene_feature_gff_to_quantify_pathogen: typing.Optional[
        str
    ] = "['gene', 'sRNA', 'tRNA', 'rRNA']",
    pathogen_gff_attribute: typing.Optional[str] = "locus_tag",
    rna_classes_to_replace_host: typing.Optional[
        str
    ] = "{base_dir}/data/RNA_classes_to_replace.csv",
) -> None:
    """
    nf-core/dualrnaseq

    Sample Description
    """

    pvc_name: str = initialize()
    nextflow_runtime(
        pvc_name=pvc_name,
        name=name,
        input=input,
        single_end=single_end,
        outdir=outdir,
        fasta_host=fasta_host,
        fasta_pathogen=fasta_pathogen,
        gff_host_genome=gff_host_genome,
        gff_host_tRNA=gff_host_tRNA,
        gff_pathogen=gff_pathogen,
        transcriptome_host=transcriptome_host,
        transcriptome_pathogen=transcriptome_pathogen,
        read_transcriptome_fasta_host_from_file=read_transcriptome_fasta_host_from_file,
        read_transcriptome_fasta_pathogen_from_file=read_transcriptome_fasta_pathogen_from_file,
        genome_host=genome_host,
        genome_pathogen=genome_pathogen,
        genomes_ignore=genomes_ignore,
        skip_fastqc=skip_fastqc,
        fastqc_params=fastqc_params,
        run_cutadapt=run_cutadapt,
        a=a,
        A=A,
        quality_cutoff=quality_cutoff,
        cutadapt_params=cutadapt_params,
        run_bbduk=run_bbduk,
        minlen=minlen,
        qtrim=qtrim,
        trimq=trimq,
        ktrim=ktrim,
        k=k,
        mink=mink,
        hdist=hdist,
        adapters=adapters,
        bbduk_params=bbduk_params,
        libtype=libtype,
        incompatPrior=incompatPrior,
        generate_salmon_uniq_ambig=generate_salmon_uniq_ambig,
        gene_feature_gff_to_create_transcriptome_host=gene_feature_gff_to_create_transcriptome_host,
        gene_feature_gff_to_create_transcriptome_pathogen=gene_feature_gff_to_create_transcriptome_pathogen,
        gene_attribute_gff_to_create_transcriptome_host=gene_attribute_gff_to_create_transcriptome_host,
        gene_attribute_gff_to_create_transcriptome_pathogen=gene_attribute_gff_to_create_transcriptome_pathogen,
        run_salmon_selective_alignment=run_salmon_selective_alignment,
        kmer_length=kmer_length,
        writeUnmappedNames=writeUnmappedNames,
        softclipOverhangs=softclipOverhangs,
        dumpEq=dumpEq,
        writeMappings=writeMappings,
        keepDuplicates=keepDuplicates,
        salmon_sa_params_index=salmon_sa_params_index,
        salmon_sa_params_mapping=salmon_sa_params_mapping,
        run_salmon_alignment_based_mode=run_salmon_alignment_based_mode,
        salmon_alignment_based_params=salmon_alignment_based_params,
        run_star=run_star,
        outSAMunmapped=outSAMunmapped,
        outSAMattributes=outSAMattributes,
        outFilterMultimapNmax=outFilterMultimapNmax,
        outFilterType=outFilterType,
        alignSJoverhangMin=alignSJoverhangMin,
        alignSJDBoverhangMin=alignSJDBoverhangMin,
        outFilterMismatchNmax=outFilterMismatchNmax,
        outFilterMismatchNoverReadLmax=outFilterMismatchNoverReadLmax,
        alignIntronMin=alignIntronMin,
        alignIntronMax=alignIntronMax,
        alignMatesGapMax=alignMatesGapMax,
        limitBAMsortRAM=limitBAMsortRAM,
        winAnchorMultimapNmax=winAnchorMultimapNmax,
        sjdbOverhang=sjdbOverhang,
        quantTranscriptomeBan=quantTranscriptomeBan,
        star_salmon_index_params=star_salmon_index_params,
        star_salmon_alignment_params=star_salmon_alignment_params,
        outWigType=outWigType,
        outWigStrand=outWigStrand,
        star_index_params=star_index_params,
        star_alignment_params=star_alignment_params,
        run_htseq_uniquely_mapped=run_htseq_uniquely_mapped,
        stranded=stranded,
        max_reads_in_buffer=max_reads_in_buffer,
        minaqual=minaqual,
        htseq_params=htseq_params,
        gene_feature_gff_to_quantify_host=gene_feature_gff_to_quantify_host,
        host_gff_attribute=host_gff_attribute,
        gene_feature_gff_to_quantify_pathogen=gene_feature_gff_to_quantify_pathogen,
        pathogen_gff_attribute=pathogen_gff_attribute,
        mapping_statistics=mapping_statistics,
        rna_classes_to_replace_host=rna_classes_to_replace_host,
    )
