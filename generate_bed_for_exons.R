## gene target regions
library(biomaRt)
library(intervals)

exon.ext = 50
target.genes = c("GCKR", "BRCA1", "BRCA2", "PALB2", "FANCD2", "MBD4")
bed.file = "targets.bed"

ensembl = useMart(biomart = "ENSEMBL_MART_ENSEMBL", host = "grch37.ensembl.org", path = "/biomart/martservice", dataset = "hsapiens_gene_ensembl")

query1 = getBM(attributes = c("chromosome_name", "exon_chrom_start", "exon_chrom_end", "ensembl_gene_id", "ensembl_exon_id"), filters = "biotype", values = "protein_coding", mart = ensembl)
query1 = query1[query1$chromosome_name %in% c(1:22, "X", "Y"),]

query2 = getBM(attributes = c("external_gene_name", "ensembl_gene_id", "gene_biotype", "source"), filters = "biotype", values = "protein_coding", mart = ensembl)

e = merge(query1, query2, by = "ensembl_gene_id", all.x = TRUE)
e$id = paste0(e$chromosome_name, "_", e$exon_chrom_start, "_", e$exon_chrom_end)
e = e[!duplicated(e$id),]

e.target = e[e$external_gene_name %in% target.genes,]
e.target.list = split(e.target[,2:4], e.target$chromosome_name)
bed = do.call(rbind, lapply(e.target.list, function(x) data.frame(x[1,1], reduce(Intervals(cbind(x[,2] - exon.ext, x[,3] + exon.ext))))))

write.table(bed, bed.file, sep = "\t", col.names = FALSE, row.names = FALSE, quote = F)
